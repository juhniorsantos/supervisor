//! A single supervised process and its state machine.
//!
//! The transitions here are a faithful port of the core logic in
//! `supervisor/process.py`:
//!
//! * `spawn` puts the process into `STARTING`.
//! * After it stays up for `startsecs` it becomes `RUNNING`.
//! * If it dies before `startsecs`, it goes to `BACKOFF` and is retried,
//!   with an increasing delay, up to `startretries` times before `FATAL`.
//! * If it dies after running, it becomes `EXITED` and may be restarted
//!   depending on `autorestart`.
//! * Stopping sends `stopsignal`, then escalates to `SIGKILL` after
//!   `stopwaitsecs`.

use std::ffi::CString;
use std::os::fd::{AsRawFd, FromRawFd, OwnedFd, RawFd};
use std::os::unix::process::CommandExt;
use std::process::{Command, Stdio};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::{AutoRestart, LogTarget, ProgramConfig};
use crate::logger::RotatingLogger;
use crate::states::ProcessState;

/// A running (or stopped) instance of a configured program.
pub struct Process {
    pub config: ProgramConfig,
    pub state: ProcessState,
    /// OS process id, or 0 when not running.
    pub pid: i32,
    /// When a delayed action (retry after backoff, or SIGKILL escalation)
    /// is due.
    delay: Option<Instant>,
    /// Number of consecutive failed start attempts.
    backoff: u32,
    /// When the current attempt was spawned.
    laststart: Option<Instant>,
    /// Wall-clock spawn / exit times, reported as epoch seconds by the API.
    laststart_sys: Option<SystemTime>,
    laststop_sys: Option<SystemTime>,
    /// Resolved stdout/stderr log paths (empty string when disabled), as the
    /// API surfaces them.
    pub stdout_path: String,
    pub stderr_path: String,
    /// Last observed exit code (negative for signal-terminated).
    pub exitstatus: Option<i32>,
    /// Human-readable reason the last start failed.
    pub spawnerr: Option<String>,
    /// Set when the user explicitly stopped the process; suppresses restart.
    administratively_stopped: bool,
    /// Set when a restart was requested: the process is stopped now and
    /// re-spawned automatically once it has fully exited.
    restart_pending: bool,
    stdout_logger: RotatingLogger,
    stderr_logger: RotatingLogger,
    stdout_read: Option<OwnedFd>,
    stderr_read: Option<OwnedFd>,
    /// Whether the most recent exit was an "expected" one (for EXITED events).
    last_exit_expected: bool,
    /// `(event_name, payload)` pairs produced by state changes, drained each
    /// tick by the supervisor and routed to event listeners.
    pub pending_events: Vec<(String, String)>,
    // --- Event listener protocol state (only used when `is_listener`) ------
    /// Write end of the child's stdin. For event listeners the supervisor
    /// writes event envelopes here; for ordinary programs it carries
    /// `sendProcessStdin` data.
    stdin_write: Option<OwnedFd>,
    pub listener_state: ListenerState,
    /// Unparsed bytes read from the listener's stdout protocol stream.
    listener_buf: Vec<u8>,
    /// Expected RESULT length while parsing a listener result, if any.
    result_len: Option<usize>,
    result_buf: Vec<u8>,
    /// Per-pool serial counter for outgoing events.
    pub pool_serial: u64,
    /// Capture-mode scanners for the stdout/stderr communication protocol.
    stdout_capture: CaptureState,
    stderr_capture: CaptureState,
    /// Optional syslog forwarders for stdout/stderr.
    stdout_syslog: Option<crate::syslog::Syslog>,
    stderr_syslog: Option<crate::syslog::Syslog>,
    /// Event names this listener subscribes to (concrete or abstract).
    subscribed: std::collections::HashSet<String>,
    /// Buffered events awaiting delivery: `(serial, name, payload)`.
    event_buffer: std::collections::VecDeque<(u64, String, String)>,
    /// An envelope currently being written to a listener's stdin, with how
    /// many of its bytes have been flushed so far. Lets a large or
    /// slow-to-drain write resume across ticks without re-sending.
    pending_write: Vec<u8>,
    pending_off: usize,
    /// For FastCGI programs: the shared listening socket (owned by the
    /// supervisor) that becomes the child's fd 0. Borrowed, not owned.
    fcgi_listen_fd: Option<RawFd>,
}

/// The state of an event listener in the notification protocol, mirroring
/// `EventListenerStates` in the original.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum ListenerState {
    /// Busy: awaiting a `READY` token before it can accept an event.
    Acknowledged,
    /// Ready to be sent an event.
    Ready,
    /// Processing an event we sent; awaiting its `RESULT`.
    Busy,
    /// Protocol desynchronised; no longer eligible for events.
    Unknown,
}

/// Streaming scanner state for the `<!--XSUPERVISOR:BEGIN-->` /
/// `<!--XSUPERVISOR:END-->` process-communication capture protocol.
#[derive(Default)]
struct CaptureState {
    /// Bytes not yet classified (may hold a partial token across reads).
    buf: Vec<u8>,
    /// Whether we are currently between BEGIN and END markers.
    in_capture: bool,
    /// Bytes captured for the current event.
    captured: Vec<u8>,
}

impl Process {
    /// Build a process from its configuration, resolving `AUTO` log paths
    /// against `childlogdir`.
    pub fn new(config: ProgramConfig, childlogdir: &std::path::Path) -> Self {
        let config_events = config.events.clone();
        let stdout_syslog_writer = config
            .stdout_syslog
            .then(|| crate::syslog::Syslog::new(&config.name));
        let stderr_syslog_writer = config
            .stderr_syslog
            .then(|| crate::syslog::Syslog::new(&config.name));
        let stdout_logger = make_logger(
            &config.stdout_logfile,
            childlogdir,
            &config.name,
            "stdout",
            config.stdout_logfile_maxbytes,
            config.stdout_logfile_backups,
        );
        let stderr_logger = make_logger(
            &config.stderr_logfile,
            childlogdir,
            &config.name,
            "stderr",
            config.stderr_logfile_maxbytes,
            config.stderr_logfile_backups,
        );
        let stdout_path = resolved_log_path(&config.stdout_logfile, childlogdir, &config.name, "stdout");
        let stderr_path = if config.redirect_stderr {
            String::new()
        } else {
            resolved_log_path(&config.stderr_logfile, childlogdir, &config.name, "stderr")
        };
        Process {
            config,
            state: ProcessState::Stopped,
            pid: 0,
            delay: None,
            backoff: 0,
            laststart: None,
            laststart_sys: None,
            laststop_sys: None,
            stdout_path,
            stderr_path,
            exitstatus: None,
            spawnerr: None,
            administratively_stopped: false,
            restart_pending: false,
            stdout_logger,
            stderr_logger,
            stdout_read: None,
            stderr_read: None,
            last_exit_expected: true,
            pending_events: Vec::new(),
            stdin_write: None,
            listener_state: ListenerState::Acknowledged,
            listener_buf: Vec::new(),
            result_len: None,
            result_buf: Vec::new(),
            pool_serial: 0,
            stdout_capture: CaptureState::default(),
            stderr_capture: CaptureState::default(),
            stdout_syslog: stdout_syslog_writer,
            stderr_syslog: stderr_syslog_writer,
            subscribed: config_events.into_iter().collect(),
            event_buffer: std::collections::VecDeque::new(),
            pending_write: Vec::new(),
            pending_off: 0,
            fcgi_listen_fd: None,
        }
    }

    pub fn is_listener(&self) -> bool {
        self.config.is_listener
    }

    /// Point this FastCGI process at the group's shared listening socket; it
    /// will be handed to the child as fd 0 on each spawn.
    pub fn set_fcgi_fd(&mut self, fd: RawFd) {
        self.fcgi_listen_fd = Some(fd);
    }

    /// The pool (group) name used in event envelopes.
    pub fn pool_name(&self) -> &str {
        &self.config.group
    }

    /// Whether this listener is currently eligible to be sent an event.
    pub fn listener_ready(&self) -> bool {
        self.config.is_listener
            && self.state == ProcessState::Running
            && self.listener_state == ListenerState::Ready
    }

    /// Whether this listener subscribes to `event_name`.
    pub fn subscribed_to(&self, event_name: &str) -> bool {
        crate::events::subscription_matches(&self.subscribed, event_name)
    }

    /// Buffer an event for later delivery, discarding the oldest if the
    /// buffer is full (matching the original's overflow behaviour).
    pub fn buffer_event(&mut self, serial: u64, name: &str, payload: &str) {
        if self.event_buffer.len() >= self.config.buffer_size {
            self.event_buffer.pop_front();
        }
        self.event_buffer
            .push_back((serial, name.to_string(), payload.to_string()));
    }

    /// The oldest buffered event, if any.
    pub fn peek_event(&self) -> Option<(u64, String, String)> {
        self.event_buffer.front().cloned()
    }

    /// Whether there is a partially-written stdin payload still being flushed.
    pub fn has_pending_write(&self) -> bool {
        !self.pending_write.is_empty()
    }

    /// Flush as much of the pending stdin write as the (non-blocking) pipe
    /// will currently accept. Safe to call repeatedly; a partial write simply
    /// resumes on the next tick.
    pub fn pump_stdin(&mut self) {
        if self.pending_write.is_empty() {
            return;
        }
        let Some(fd) = self.stdin_write.as_ref() else {
            self.pending_write.clear();
            self.pending_off = 0;
            return;
        };
        let raw = fd.as_raw_fd();
        while self.pending_off < self.pending_write.len() {
            let chunk = &self.pending_write[self.pending_off..];
            let n =
                unsafe { libc::write(raw, chunk.as_ptr() as *const libc::c_void, chunk.len()) };
            if n > 0 {
                self.pending_off += n as usize;
            } else {
                // EAGAIN (pipe full) or EPIPE (closed): stop and retry later.
                break;
            }
        }
        if self.pending_off >= self.pending_write.len() {
            self.pending_write.clear();
            self.pending_off = 0;
        } else if self.pending_off > 0 {
            // Compact: drop the bytes already flushed so the buffer only ever
            // holds the outstanding tail (bounds memory for slow readers).
            self.pending_write.drain(..self.pending_off);
            self.pending_off = 0;
        }
    }

    /// Begin delivering an event envelope to the listener: consume the
    /// buffered event, mark the listener BUSY, and start flushing. Because the
    /// listener is now BUSY it won't be handed another event until its RESULT
    /// arrives, so even a slow/partial write can never interleave two events.
    pub fn begin_send_event(&mut self, envelope: Vec<u8>) {
        self.event_buffer.pop_front();
        self.listener_state = ListenerState::Busy;
        self.pending_write = envelope;
        self.pending_off = 0;
        self.pump_stdin();
    }

    /// Maximum amount of unflushed stdin data we will buffer before refusing
    /// more, to bound memory if the child never reads.
    const MAX_STDIN_BUFFER: usize = 1 << 20; // 1 MiB

    /// Queue `data` for the process's stdin and flush as much as the pipe will
    /// accept now; the remainder is drained on subsequent ticks. Returns the
    /// number of bytes accepted, or `None` if there is no stdin (not running)
    /// or the buffer is full.
    pub fn write_stdin(&mut self, data: &[u8]) -> Option<usize> {
        self.stdin_write.as_ref()?; // no stdin -> not running
        let unflushed = self.pending_write.len() - self.pending_off;
        if unflushed + data.len() > Self::MAX_STDIN_BUFFER {
            return None;
        }
        self.pending_write.extend_from_slice(data);
        self.pump_stdin();
        Some(data.len())
    }

    /// Send a UNIX signal to the running process (its process group). Returns
    /// true if the process is in a signallable state.
    pub fn signal(&self, sig: i32) -> bool {
        if !matches!(
            self.state,
            ProcessState::Running | ProcessState::Starting | ProcessState::Stopping
        ) {
            return false;
        }
        self.send_signal(sig);
        true
    }

    /// Truncate this process's stdout/stderr logs (and their rotations).
    pub fn clear_logs(&mut self) {
        self.stdout_logger.clear();
        self.stderr_logger.clear();
    }

    pub fn name(&self) -> &str {
        &self.config.name
    }

    /// Spawn the child process and enter the `STARTING` state.
    pub fn spawn(&mut self, now: Instant) {
        let argv = match shell_split(&self.config.command) {
            Ok(a) if !a.is_empty() => a,
            _ => {
                self.spawnerr = Some("bad command".to_string());
                self.fail_to_backoff(now);
                return;
            }
        };

        // stdout pipe (always); stderr pipe unless redirected onto stdout.
        let (out_r, out_w) = match make_pipe() {
            Ok(p) => p,
            Err(e) => {
                self.spawnerr = Some(format!("pipe failed: {e}"));
                self.fail_to_backoff(now);
                return;
            }
        };

        let mut cmd = Command::new(&argv[0]);
        cmd.args(&argv[1..]);

        // FastCGI programs get the group's shared listening socket as fd 0;
        // everything else gets a stdin pipe we keep the write end of (event
        // listeners receive event envelopes there, ordinary programs receive
        // `sendProcessStdin` data). The child's read end stays blocking; our
        // write end is non-blocking so we never stall the event loop.
        let stdin_write = if let Some(raw) = self.fcgi_listen_fd {
            let dup = unsafe { libc::dup(raw) };
            if dup < 0 {
                self.spawnerr = Some("dup of fcgi socket failed".to_string());
                self.fail_to_backoff(now);
                return;
            }
            cmd.stdin(Stdio::from(unsafe { OwnedFd::from_raw_fd(dup) }));
            None
        } else {
            match make_stdin_pipe() {
                Ok((stdin_read, w)) => {
                    cmd.stdin(Stdio::from(stdin_read));
                    Some(w)
                }
                Err(e) => {
                    self.spawnerr = Some(format!("pipe failed: {e}"));
                    self.fail_to_backoff(now);
                    return;
                }
            }
        };

        let mut err_read: Option<OwnedFd> = None;
        if self.config.redirect_stderr {
            match out_w.try_clone() {
                Ok(dup) => {
                    cmd.stderr(Stdio::from(dup));
                }
                Err(e) => {
                    self.spawnerr = Some(format!("dup failed: {e}"));
                    self.fail_to_backoff(now);
                    return;
                }
            }
        } else {
            match make_pipe() {
                Ok((er, ew)) => {
                    cmd.stderr(Stdio::from(ew));
                    err_read = Some(er);
                }
                Err(e) => {
                    self.spawnerr = Some(format!("pipe failed: {e}"));
                    self.fail_to_backoff(now);
                    return;
                }
            }
        }
        cmd.stdout(Stdio::from(out_w));

        if let Some(dir) = &self.config.directory {
            cmd.current_dir(dir);
        }
        for (k, v) in &self.config.environment {
            cmd.env(k, v);
        }

        // Resolve setuid target in the parent (getpwnam allocates).
        let uid_gid = match &self.config.user {
            Some(u) => match resolve_user(u) {
                Ok(ids) => Some(ids),
                Err(e) => {
                    self.spawnerr = Some(e);
                    self.fail_to_backoff(now);
                    return;
                }
            },
            None => None,
        };
        let umask = self.config.umask;

        // Detach into a new session/process-group and drop privileges.
        unsafe {
            cmd.pre_exec(move || {
                libc::setsid();
                if let Some(m) = umask {
                    libc::umask(m as libc::mode_t);
                }
                if let Some((uid, gid)) = uid_gid {
                    if libc::setgid(gid) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                    if libc::setuid(uid) != 0 {
                        return Err(std::io::Error::last_os_error());
                    }
                }
                Ok(())
            });
        }

        match cmd.spawn() {
            Ok(child) => {
                self.pid = child.id() as i32;
                // We reap children ourselves via waitpid(); prevent std from
                // closing/owning anything by forgetting the handle. Its
                // Drop would not reap on Unix anyway, but this is explicit.
                std::mem::forget(child);
                self.laststart = Some(now);
                self.laststart_sys = Some(SystemTime::now());
                self.delay = Some(now + Duration::from_secs(self.config.startsecs));
                self.spawnerr = None;
                self.stdout_read = Some(out_r);
                self.stderr_read = err_read;
                self.stdin_write = stdin_write;
                if self.config.is_listener {
                    self.listener_state = ListenerState::Acknowledged;
                    self.listener_buf.clear();
                    self.result_len = None;
                    self.result_buf.clear();
                }
                self.change_state(ProcessState::Starting);
            }
            Err(e) => {
                self.spawnerr = Some(format!("spawn failed: {e}"));
                self.fail_to_backoff(now);
            }
        }
    }

    fn change_state(&mut self, new: ProcessState) {
        let from = self.state;
        self.state = new;
        // Record a PROCESS_STATE_* event for the listener subsystem.
        let name = crate::events::process_state_event_name(new);
        let payload = crate::events::process_state_payload(
            &self.config.name,
            &self.config.group,
            from,
            new,
            self.pid,
            self.backoff,
            self.last_exit_expected,
        );
        self.pending_events.push((name.to_string(), payload));
        // If a listener leaves RUNNING, its protocol state is no longer valid.
        if self.config.is_listener && new != ProcessState::Running {
            self.listener_state = ListenerState::Acknowledged;
            self.listener_buf.clear();
            self.result_len = None;
            self.result_buf.clear();
            self.pending_write.clear();
            self.pending_off = 0;
        }
    }

    /// Enter BACKOFF (a failed/too-quick start): bump the counter and set
    /// the retry delay to `backoff` seconds.
    fn fail_to_backoff(&mut self, now: Instant) {
        self.backoff += 1;
        self.delay = Some(now + Duration::from_secs(self.backoff as u64));
        self.change_state(ProcessState::Backoff);
    }

    /// Called when this process has been reaped. `raw_status` is the raw
    /// status from `waitpid`.
    pub fn on_reap(&mut self, raw_status: i32, now: Instant) {
        // Drain any final output and close the pipes.
        self.drain_output();
        self.stdout_read = None;
        self.stderr_read = None;

        let (es, signaled) = decode_status(raw_status);
        let expected = !signaled && self.config.exitcodes.contains(&es);
        self.last_exit_expected = expected;
        self.laststop_sys = Some(SystemTime::now());
        // The stdin pipe is gone once the process exits; drop any half-written
        // event envelope with it.
        self.stdin_write = None;
        self.pending_write.clear();
        self.pending_off = 0;

        match self.state {
            ProcessState::Starting => {
                let too_quickly = self
                    .laststart
                    .map(|t| now.duration_since(t) < Duration::from_secs(self.config.startsecs))
                    .unwrap_or(true);
                if too_quickly {
                    self.exitstatus = Some(es);
                    self.spawnerr =
                        Some("Exited too quickly (process log may have details)".to_string());
                    self.fail_to_backoff(now);
                } else {
                    self.exitstatus = Some(es);
                    self.backoff = 0;
                    self.delay = None;
                    self.change_state(ProcessState::Exited);
                }
            }
            ProcessState::Running => {
                self.exitstatus = Some(es);
                self.backoff = 0;
                self.delay = None;
                self.change_state(ProcessState::Exited);
            }
            ProcessState::Stopping => {
                self.exitstatus = Some(es);
                self.backoff = 0;
                self.delay = None;
                self.change_state(ProcessState::Stopped);
            }
            _ => {
                self.exitstatus = Some(es);
                self.change_state(ProcessState::Stopped);
            }
        }
        // The event payloads above captured the pid; clear it now.
        self.pid = 0;
    }

    /// Advance the state machine. Called every tick.
    pub fn transition(&mut self, now: Instant, shutting_down: bool) {
        match self.state {
            ProcessState::Starting => {
                let up_long_enough = self
                    .laststart
                    .map(|t| now.duration_since(t) >= Duration::from_secs(self.config.startsecs))
                    .unwrap_or(false);
                if up_long_enough {
                    self.spawnerr = None;
                    self.backoff = 0;
                    self.delay = None;
                    self.change_state(ProcessState::Running);
                }
            }
            ProcessState::Backoff => {
                if self.backoff > self.config.startretries {
                    self.delay = None;
                    self.spawnerr = Some(format!(
                        "Exited too quickly; gave up after {} retries",
                        self.config.startretries
                    ));
                    self.change_state(ProcessState::Fatal);
                } else if !shutting_down {
                    if let Some(d) = self.delay {
                        if now >= d {
                            self.spawn(now);
                        }
                    }
                }
            }
            ProcessState::Exited => {
                if !shutting_down && !self.administratively_stopped && self.should_restart() {
                    // Throttle flapping: if the last run was very short (e.g.
                    // a startsecs=0 program that exits immediately), wait until
                    // at least one second has passed since it started before
                    // respawning, so autorestart can't busy-loop. Long-running
                    // processes restart immediately.
                    if let Some(started) = self.laststart {
                        if now < started + Duration::from_secs(1) {
                            return;
                        }
                    }
                    self.spawn(now);
                }
            }
            ProcessState::Stopping => {
                if let Some(d) = self.delay {
                    if now >= d && self.pid != 0 {
                        // stopwaitsecs elapsed; escalate to SIGKILL.
                        self.send_signal(libc::SIGKILL);
                        self.delay = Some(now + Duration::from_secs(2));
                    }
                }
            }
            ProcessState::Stopped => {
                // A restart was requested while the process was running; the
                // stop has now completed, so bring it back up.
                if self.restart_pending && !shutting_down {
                    self.restart_pending = false;
                    self.administratively_stopped = false;
                    self.backoff = 0;
                    self.delay = None;
                    self.spawn(now);
                }
            }
            _ => {}
        }
    }

    /// Request a restart: stop now if running, then re-spawn automatically
    /// once the process has exited. If already stopped, start immediately.
    pub fn request_restart(&mut self, now: Instant) {
        match self.state {
            ProcessState::Running | ProcessState::Starting | ProcessState::Stopping => {
                self.restart_pending = true;
                self.stop(now);
            }
            _ => {
                self.start(now);
            }
        }
    }

    fn should_restart(&self) -> bool {
        match self.config.autorestart {
            AutoRestart::Never => false,
            AutoRestart::Always => true,
            AutoRestart::Unexpected => match self.exitstatus {
                Some(es) => !(es >= 0 && self.config.exitcodes.contains(&es)),
                None => true,
            },
        }
    }

    /// Begin a graceful stop. Returns true if a stop was initiated.
    pub fn stop(&mut self, now: Instant) -> bool {
        self.administratively_stopped = true;
        match self.state {
            ProcessState::Running | ProcessState::Starting => {
                self.send_signal(self.config.stopsignal);
                self.delay = Some(now + Duration::from_secs(self.config.stopwaitsecs));
                self.change_state(ProcessState::Stopping);
                true
            }
            ProcessState::Stopping => true, // already stopping
            ProcessState::Backoff => {
                self.delay = None;
                self.change_state(ProcessState::Stopped);
                true
            }
            _ => false,
        }
    }

    /// Handle a user start request. Returns true if a start was initiated.
    pub fn start(&mut self, now: Instant) -> bool {
        match self.state {
            ProcessState::Running | ProcessState::Starting | ProcessState::Stopping => false,
            _ => {
                self.administratively_stopped = false;
                self.backoff = 0;
                self.delay = None;
                self.spawn(now);
                true
            }
        }
    }

    fn send_signal(&self, sig: i32) {
        if self.pid <= 0 {
            return;
        }
        // Signal the whole process group (the child called setsid, so its
        // pgid equals its pid). Fall back to the bare pid if the group is
        // gone.
        unsafe {
            if libc::kill(-self.pid, sig) != 0 {
                libc::kill(self.pid, sig);
            }
        }
    }

    /// Read whatever output is currently buffered from the child's pipes
    /// (non-blocking). For ordinary programs both streams go to the rotating
    /// loggers; for event listeners, stdout drives the notification protocol
    /// while stderr is still logged.
    pub fn drain_output(&mut self) {
        if self.config.is_listener {
            if let Some(fd) = self.stdout_read.as_ref() {
                let raw = fd.as_raw_fd();
                let mut buf = [0u8; 4096];
                loop {
                    let n = unsafe {
                        libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                    };
                    if n > 0 {
                        self.listener_buf.extend_from_slice(&buf[..n as usize]);
                    } else {
                        break;
                    }
                }
                self.advance_listener_protocol();
            }
            if let Some(fd) = self.stderr_read.as_ref() {
                drain_fd(fd.as_raw_fd(), &mut self.stderr_logger);
            }
        } else {
            self.drain_stream(true);
            self.drain_stream(false);
        }
    }

    /// Drain one ordinary-program stream. When capture is enabled, scan for
    /// the communication markers and emit `PROCESS_COMMUNICATION_*` events for
    /// captured spans; otherwise append straight to the logger.
    fn drain_stream(&mut self, is_stdout: bool) {
        let fd = if is_stdout {
            self.stdout_read.as_ref()
        } else {
            self.stderr_read.as_ref()
        };
        let Some(fd) = fd else { return };
        let raw = fd.as_raw_fd();

        let maxbytes = if is_stdout {
            self.config.stdout_capture_maxbytes
        } else {
            self.config.stderr_capture_maxbytes
        };
        let syslog_enabled = if is_stdout {
            self.stdout_syslog.is_some()
        } else {
            self.stderr_syslog.is_some()
        };

        // Fast path: no capture and no syslog — stream straight to the logger.
        if maxbytes == 0 && !syslog_enabled {
            let logger = if is_stdout {
                &mut self.stdout_logger
            } else {
                &mut self.stderr_logger
            };
            drain_fd(raw, logger);
            return;
        }

        // Read all available bytes, then fan out to syslog/capture/logger.
        let mut data = Vec::new();
        let mut buf = [0u8; 8192];
        loop {
            let n = unsafe { libc::read(raw, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n > 0 {
                data.extend_from_slice(&buf[..n as usize]);
            } else {
                break;
            }
        }
        if data.is_empty() {
            return;
        }

        // Forward to syslog (a separate field, so no borrow conflict below).
        let syslog = if is_stdout {
            self.stdout_syslog.as_mut()
        } else {
            self.stderr_syslog.as_mut()
        };
        if let Some(sl) = syslog {
            sl.feed(&data);
        }

        if maxbytes == 0 {
            let logger = if is_stdout {
                &mut self.stdout_logger
            } else {
                &mut self.stderr_logger
            };
            logger.write(&data);
            return;
        }

        let (state, logger) = if is_stdout {
            (&mut self.stdout_capture, &mut self.stdout_logger)
        } else {
            (&mut self.stderr_capture, &mut self.stderr_logger)
        };
        let completed = scan_capture(state, &data, maxbytes as usize, logger);

        let event_name = if is_stdout {
            "PROCESS_COMMUNICATION_STDOUT"
        } else {
            "PROCESS_COMMUNICATION_STDERR"
        };
        for captured in completed {
            let payload = format!(
                "processname:{} groupname:{} pid:{}\n{}",
                self.config.name,
                self.config.group,
                self.pid,
                String::from_utf8_lossy(&captured)
            );
            self.pending_events.push((event_name.to_string(), payload));
        }
    }

    /// Advance the event-listener protocol state machine over whatever bytes
    /// have accumulated in `listener_buf` (the READY / RESULT handshake).
    fn advance_listener_protocol(&mut self) {
        const READY: &[u8] = b"READY\n";
        const RESULT: &[u8] = b"RESULT ";
        loop {
            match self.listener_state {
                ListenerState::Acknowledged => {
                    if self.listener_buf.starts_with(READY) {
                        self.listener_buf.drain(..READY.len());
                        self.listener_state = ListenerState::Ready;
                    } else if self.listener_buf.len() >= READY.len() {
                        self.listener_state = ListenerState::Unknown;
                        break;
                    } else {
                        break;
                    }
                }
                ListenerState::Ready => break,
                ListenerState::Busy => {
                    if self.result_len.is_none() {
                        let Some(nl) = self.listener_buf.iter().position(|&b| b == b'\n') else {
                            break;
                        };
                        let line = self.listener_buf[..nl].to_vec();
                        if line.starts_with(RESULT) {
                            let num = String::from_utf8_lossy(&line[RESULT.len()..]);
                            match num.trim().parse::<usize>() {
                                Ok(len) => {
                                    self.listener_buf.drain(..=nl);
                                    self.result_len = Some(len);
                                }
                                Err(_) => {
                                    self.listener_state = ListenerState::Unknown;
                                    break;
                                }
                            }
                        } else {
                            self.listener_state = ListenerState::Unknown;
                            break;
                        }
                    } else {
                        let want = self.result_len.unwrap();
                        let need = want - self.result_buf.len();
                        let take = need.min(self.listener_buf.len());
                        let chunk: Vec<u8> = self.listener_buf.drain(..take).collect();
                        self.result_buf.extend_from_slice(&chunk);
                        if self.result_buf.len() >= want {
                            // Result received (OK/FAIL); ready for the next event
                            // once the listener writes READY again.
                            self.result_len = None;
                            self.result_buf.clear();
                            self.listener_state = ListenerState::Acknowledged;
                        } else {
                            break;
                        }
                    }
                }
                ListenerState::Unknown => break,
            }
        }
    }

    /// One-line status description for logs. Uptime is computed from the same
    /// wall-clock start time the API snapshot ([`Process::info`]) uses, so a
    /// process reads the same uptime everywhere.
    pub fn status_description(&self, now_epoch: i64) -> String {
        match self.state {
            ProcessState::Running => {
                let start = self
                    .laststart_sys
                    .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
                    .map(|d| d.as_secs() as i64)
                    .unwrap_or(now_epoch);
                let up = (now_epoch - start).max(0) as u64;
                format!("pid {}, uptime {}", self.pid, fmt_duration(Duration::from_secs(up)))
            }
            ProcessState::Stopped => "Not started".to_string(),
            ProcessState::Starting => "starting".to_string(),
            ProcessState::Stopping => "stopping".to_string(),
            ProcessState::Backoff => format!(
                "Backing off, retry {} of {}",
                self.backoff,
                self.config.startretries + 1
            ),
            ProcessState::Exited => match self.exitstatus {
                Some(es) if es >= 0 => format!("exited, status {es}"),
                Some(es) => format!("terminated by signal {}", -es),
                None => "exited".to_string(),
            },
            ProcessState::Fatal => self
                .spawnerr
                .clone()
                .unwrap_or_else(|| "Exited too many times".to_string()),
            ProcessState::Unknown => String::new(),
        }
    }

    /// Produce an API snapshot of this process, matching the fields and the
    /// `description` formatting of the original `getProcessInfo`.
    pub fn info(&self, now_epoch: i64) -> ProcessInfo {
        let start = self
            .laststart_sys
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let stop = self
            .laststop_sys
            .and_then(|t| t.duration_since(UNIX_EPOCH).ok())
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0);
        let description = self.interpret_description(start, stop, now_epoch);
        ProcessInfo {
            name: self.config.name.clone(),
            group: self.config.group.clone(),
            state: self.state as i64,
            statename: self.state.description().to_string(),
            pid: self.pid,
            start,
            stop,
            now: now_epoch,
            exitstatus: self.exitstatus.unwrap_or(0),
            spawnerr: self.spawnerr.clone().unwrap_or_default(),
            stdout_logfile: self.stdout_path.clone(),
            stderr_logfile: self.stderr_path.clone(),
            description,
        }
    }

    /// Port of `rpcinterface._interpretProcessInfo`.
    fn interpret_description(&self, start: i64, stop: i64, now: i64) -> String {
        match self.state {
            ProcessState::Running => {
                let uptime = (now - start).max(0);
                format!(
                    "pid {}, uptime {}",
                    self.pid,
                    fmt_duration(Duration::from_secs(uptime as u64))
                )
            }
            ProcessState::Fatal | ProcessState::Backoff => self
                .spawnerr
                .clone()
                .filter(|s| !s.is_empty())
                .unwrap_or_else(|| format!("unknown error (try \"tail {}\")", self.config.name)),
            ProcessState::Stopped | ProcessState::Exited => {
                if start > 0 {
                    fmt_stop_time(stop)
                } else {
                    "Not started".to_string()
                }
            }
            _ => String::new(),
        }
    }
}

/// An API-facing snapshot of a process's state (mirrors the struct returned
/// by the original `supervisor.getProcessInfo`).
#[derive(Clone, Debug)]
pub struct ProcessInfo {
    pub name: String,
    pub group: String,
    pub state: i64,
    pub statename: String,
    pub pid: i32,
    pub start: i64,
    pub stop: i64,
    pub now: i64,
    pub exitstatus: i32,
    pub spawnerr: String,
    pub stdout_logfile: String,
    pub stderr_logfile: String,
    pub description: String,
}

fn make_logger(
    target: &LogTarget,
    childlogdir: &std::path::Path,
    name: &str,
    stream: &str,
    maxbytes: u64,
    backups: u32,
) -> RotatingLogger {
    let path = match target {
        LogTarget::None => return RotatingLogger::null(),
        LogTarget::Auto => childlogdir.join(format!("{name}-{stream}.log")),
        LogTarget::Path(p) => p.clone(),
    };
    RotatingLogger::new(Some(path), maxbytes, backups).unwrap_or_else(|_| RotatingLogger::null())
}

/// The on-disk path a stream's log resolves to (empty when disabled),
/// matching `make_logger`'s resolution but as a string for the API.
fn resolved_log_path(
    target: &LogTarget,
    childlogdir: &std::path::Path,
    name: &str,
    stream: &str,
) -> String {
    match target {
        LogTarget::None => String::new(),
        LogTarget::Auto => childlogdir
            .join(format!("{name}-{stream}.log"))
            .to_string_lossy()
            .into_owned(),
        LogTarget::Path(p) => p.to_string_lossy().into_owned(),
    }
}

/// Format an epoch as `%b %d %I:%M %p` (e.g. `Jun 17 09:05 PM`) in local
/// time, matching the original's stopped/exited description.
fn fmt_stop_time(epoch: i64) -> String {
    const MONTHS: [&str; 12] = [
        "Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
    ];
    let t = epoch as libc::time_t;
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&t, &mut tm) };
    let mon = MONTHS.get(tm.tm_mon as usize).copied().unwrap_or("Jan");
    let hour12 = match tm.tm_hour % 12 {
        0 => 12,
        h => h,
    };
    let ampm = if tm.tm_hour < 12 { "AM" } else { "PM" };
    format!(
        "{} {:02} {:02}:{:02} {}",
        mon, tm.tm_mday, hour12, tm.tm_min, ampm
    )
}

/// Decode a `waitpid` status into `(exit_code, was_signaled)`. For a
/// signal-terminated child the code is the negated signal number so it can
/// never accidentally match a configured `exitcode`.
fn decode_status(status: i32) -> (i32, bool) {
    // Mirror the WIFEXITED / WEXITSTATUS / WTERMSIG macros.
    let low = status & 0x7f;
    if low == 0 {
        // exited normally
        ((status >> 8) & 0xff, false)
    } else if low != 0x7f {
        // terminated by signal
        (-(low), true)
    } else {
        // stopped (shouldn't happen without WUNTRACED)
        (0, false)
    }
}

const CAPTURE_BEGIN: &[u8] = b"<!--XSUPERVISOR:BEGIN-->";
const CAPTURE_END: &[u8] = b"<!--XSUPERVISOR:END-->";

/// Feed `incoming` bytes through the capture scanner. Bytes outside
/// BEGIN/END markers are written to `logger`; bytes inside are accumulated
/// (capped at `maxbytes`). Returns the payloads of any completed captures.
fn scan_capture(
    state: &mut CaptureState,
    incoming: &[u8],
    maxbytes: usize,
    logger: &mut RotatingLogger,
) -> Vec<Vec<u8>> {
    let mut completed = Vec::new();
    state.buf.extend_from_slice(incoming);

    loop {
        if !state.in_capture {
            if let Some(i) = crate::util::find_subslice(&state.buf, CAPTURE_BEGIN) {
                logger.write(&state.buf[..i]);
                state.buf.drain(..i + CAPTURE_BEGIN.len());
                state.in_capture = true;
            } else {
                // Flush all but a possible split BEGIN token at the tail.
                let hold = prefix_overlap(&state.buf, CAPTURE_BEGIN);
                let flush_to = state.buf.len() - hold;
                logger.write(&state.buf[..flush_to]);
                state.buf.drain(..flush_to);
                break;
            }
        } else if let Some(i) = crate::util::find_subslice(&state.buf, CAPTURE_END) {
            append_capped(&mut state.captured, &state.buf[..i], maxbytes);
            state.buf.drain(..i + CAPTURE_END.len());
            state.in_capture = false;
            completed.push(std::mem::take(&mut state.captured));
        } else {
            let hold = prefix_overlap(&state.buf, CAPTURE_END);
            let take_to = state.buf.len() - hold;
            append_capped(&mut state.captured, &state.buf[..take_to], maxbytes);
            state.buf.drain(..take_to);
            break;
        }
    }
    completed
}

fn append_capped(dst: &mut Vec<u8>, src: &[u8], maxbytes: usize) {
    let room = maxbytes.saturating_sub(dst.len());
    if room > 0 {
        let take = room.min(src.len());
        dst.extend_from_slice(&src[..take]);
    }
}

/// Length of the longest suffix of `buf` that is a prefix of `token`.
fn prefix_overlap(buf: &[u8], token: &[u8]) -> usize {
    let max = token.len().saturating_sub(1).min(buf.len());
    for k in (1..=max).rev() {
        if buf[buf.len() - k..] == token[..k] {
            return k;
        }
    }
    0
}

fn drain_fd(fd: i32, logger: &mut RotatingLogger) {
    let mut buf = [0u8; 8192];
    loop {
        let n = unsafe { libc::read(fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
        if n > 0 {
            logger.write(&buf[..n as usize]);
        } else {
            // 0 = EOF, <0 = EWOULDBLOCK or error; either way, stop for now.
            break;
        }
    }
}

/// Create a pipe with both ends close-on-exec and the read end
/// non-blocking. Returns `(read, write)`.
fn make_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
    use std::os::fd::FromRawFd;
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let read = fds[0];
    let write = fds[1];
    // Make the read end non-blocking so draining never stalls the loop.
    unsafe {
        let flags = libc::fcntl(read, libc::F_GETFL);
        libc::fcntl(read, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    let read = unsafe { OwnedFd::from_raw_fd(read) };
    let write = unsafe { OwnedFd::from_raw_fd(write) };
    Ok((read, write))
}

/// Create a close-on-exec pipe for a child's stdin: the read end (handed to
/// the child) stays blocking so the child reads normally, while our write end
/// is non-blocking so writing never stalls the event loop. Returns
/// `(read, write)`.
fn make_stdin_pipe() -> std::io::Result<(OwnedFd, OwnedFd)> {
    use std::os::fd::FromRawFd;
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC) };
    if rc != 0 {
        return Err(std::io::Error::last_os_error());
    }
    let write = fds[1];
    unsafe {
        let flags = libc::fcntl(write, libc::F_GETFL);
        libc::fcntl(write, libc::F_SETFL, flags | libc::O_NONBLOCK);
    }
    let read = unsafe { OwnedFd::from_raw_fd(fds[0]) };
    let write = unsafe { OwnedFd::from_raw_fd(write) };
    Ok((read, write))
}

fn resolve_user(name: &str) -> Result<(u32, u32), String> {
    // Allow a numeric uid as well as a name.
    if let Ok(uid) = name.parse::<u32>() {
        return Ok((uid, uid));
    }
    let cname = CString::new(name).map_err(|_| "invalid user name".to_string())?;
    let pw = unsafe { libc::getpwnam(cname.as_ptr()) };
    if pw.is_null() {
        Err(format!("unknown user: {name}"))
    } else {
        unsafe { Ok(((*pw).pw_uid, (*pw).pw_gid)) }
    }
}

/// Split a command line into argv, honouring single and double quotes. This
/// is a small shell-like splitter, not a full shell.
fn shell_split(input: &str) -> Result<Vec<String>, String> {
    let mut args = Vec::new();
    let mut cur = String::new();
    let mut chars = input.chars().peekable();
    let mut in_arg = false;

    while let Some(c) = chars.next() {
        match c {
            ' ' | '\t' | '\n' | '\r' => {
                if in_arg {
                    args.push(std::mem::take(&mut cur));
                    in_arg = false;
                }
            }
            '\'' => {
                in_arg = true;
                for q in chars.by_ref() {
                    if q == '\'' {
                        break;
                    }
                    cur.push(q);
                }
            }
            '"' => {
                in_arg = true;
                while let Some(q) = chars.next() {
                    if q == '"' {
                        break;
                    }
                    if q == '\\' {
                        if let Some(&next) = chars.peek() {
                            if next == '"' || next == '\\' {
                                cur.push(next);
                                chars.next();
                                continue;
                            }
                        }
                    }
                    cur.push(q);
                }
            }
            '\\' => {
                in_arg = true;
                if let Some(next) = chars.next() {
                    cur.push(next);
                }
            }
            _ => {
                in_arg = true;
                cur.push(c);
            }
        }
    }
    if in_arg {
        args.push(cur);
    }
    Ok(args)
}

/// Format a duration as `H:MM:SS`.
fn fmt_duration(d: Duration) -> String {
    let secs = d.as_secs();
    let h = secs / 3600;
    let m = (secs % 3600) / 60;
    let s = secs % 60;
    format!("{h}:{m:02}:{s:02}")
}

#[cfg(test)]
mod tests {
    use super::*;

    // --- State-machine test helpers ---------------------------------------
    //
    // These build a Process without ever spawning a child, then drive
    // on_reap()/transition()/stop() directly. on_reap and transition perform
    // no fork; the only system call risk is signalling, which we avoid by
    // leaving pid == 0 (send_signal is a no-op for pid <= 0).

    fn make_proc(extra: &str) -> Process {
        let text = format!("[program:t]\ncommand=/bin/true\n{extra}");
        let cfg = crate::config::Config::parse(&text).unwrap();
        Process::new(cfg.programs[0].clone(), std::path::Path::new("/tmp"))
    }

    fn make_listener(extra: &str) -> Process {
        let text = format!("[eventlistener:t]\ncommand=/bin/cat\n{extra}");
        let cfg = crate::config::Config::parse(&text).unwrap();
        Process::new(cfg.programs[0].clone(), std::path::Path::new("/tmp"))
    }

    /// Encode a `waitpid` status for a normal exit with `code`.
    fn exited(code: i32) -> i32 {
        (code & 0xff) << 8
    }

    #[test]
    fn reap_running_clean_exit_goes_exited() {
        let mut p = make_proc("autorestart=false");
        p.state = ProcessState::Running;
        p.pid = 0;
        p.laststart = Some(Instant::now());
        p.on_reap(exited(0), Instant::now());
        assert_eq!(p.state, ProcessState::Exited);
        assert_eq!(p.exitstatus, Some(0));
        assert_eq!(p.pid, 0);
        assert!(p.last_exit_expected);
    }

    #[test]
    fn reap_running_signal_is_unexpected() {
        let mut p = make_proc("");
        p.state = ProcessState::Running;
        p.laststart = Some(Instant::now());
        p.on_reap(9, Instant::now()); // raw status: killed by signal 9
        assert_eq!(p.state, ProcessState::Exited);
        assert_eq!(p.exitstatus, Some(-9));
        assert!(!p.last_exit_expected);
    }

    #[test]
    fn reap_starting_too_quickly_backs_off() {
        let mut p = make_proc("startsecs=5\nstartretries=3");
        p.state = ProcessState::Starting;
        p.laststart = Some(Instant::now()); // just started -> "too quickly"
        p.backoff = 0;
        p.on_reap(exited(1), Instant::now());
        assert_eq!(p.state, ProcessState::Backoff);
        assert_eq!(p.backoff, 1);
        assert!(p.delay.is_some());
        assert!(p.spawnerr.is_some());
    }

    #[test]
    fn reap_starting_after_startsecs_exits() {
        let mut p = make_proc("startsecs=1");
        p.state = ProcessState::Starting;
        p.laststart = Some(Instant::now() - Duration::from_secs(5));
        p.on_reap(exited(0), Instant::now());
        assert_eq!(p.state, ProcessState::Exited);
    }

    #[test]
    fn reap_stopping_goes_stopped() {
        let mut p = make_proc("");
        p.state = ProcessState::Stopping;
        p.on_reap(exited(0), Instant::now());
        assert_eq!(p.state, ProcessState::Stopped);
    }

    #[test]
    fn transition_starting_to_running_emits_event() {
        let mut p = make_proc("startsecs=0");
        p.state = ProcessState::Starting;
        p.laststart = Some(Instant::now() - Duration::from_secs(1));
        p.pid = 4321;
        p.pending_events.clear();
        p.transition(Instant::now(), false);
        assert_eq!(p.state, ProcessState::Running);
        let (name, payload) = p.pending_events.last().unwrap();
        assert_eq!(name, "PROCESS_STATE_RUNNING");
        assert!(payload.contains("pid:4321"));
        assert!(payload.contains("from_state:STARTING"));
    }

    #[test]
    fn transition_backoff_exceeding_retries_goes_fatal() {
        let mut p = make_proc("startretries=2");
        p.state = ProcessState::Backoff;
        p.backoff = 3; // 3 > 2
        p.transition(Instant::now(), false);
        assert_eq!(p.state, ProcessState::Fatal);
        assert!(p.spawnerr.is_some());
    }

    #[test]
    fn backoff_then_stop_cancels_to_stopped() {
        // Drive the full too-quick-exit -> backoff -> stop path.
        let mut p = make_proc("startsecs=5");
        p.state = ProcessState::Starting;
        p.laststart = Some(Instant::now());
        p.on_reap(exited(1), Instant::now());
        assert_eq!(p.state, ProcessState::Backoff);
        // A stop request while backing off must abandon retries immediately.
        assert!(p.stop(Instant::now()));
        assert_eq!(p.state, ProcessState::Stopped);
    }

    #[test]
    fn should_restart_honours_autorestart_modes() {
        let mut never = make_proc("autorestart=false");
        never.exitstatus = Some(3);
        assert!(!never.should_restart());

        let mut always = make_proc("autorestart=true");
        always.exitstatus = Some(0);
        assert!(always.should_restart());

        let mut unexpected = make_proc("autorestart=unexpected\nexitcodes=0,2");
        unexpected.exitstatus = Some(0);
        assert!(!unexpected.should_restart(), "expected code must not restart");
        unexpected.exitstatus = Some(2);
        assert!(!unexpected.should_restart(), "listed code must not restart");
        unexpected.exitstatus = Some(5);
        assert!(unexpected.should_restart(), "unlisted code restarts");
        unexpected.exitstatus = Some(-15);
        assert!(unexpected.should_restart(), "signal death restarts");
    }

    #[test]
    fn stop_running_enters_stopping_without_signalling_real_pid() {
        let mut p = make_proc("");
        p.state = ProcessState::Running;
        p.pid = 0; // keep send_signal a no-op so no real process is touched
        assert!(p.stop(Instant::now()));
        assert_eq!(p.state, ProcessState::Stopping);
        assert!(p.administratively_stopped);
    }

    #[test]
    fn administratively_stopped_process_is_not_autorestarted() {
        let mut p = make_proc("autorestart=true");
        p.state = ProcessState::Running;
        p.pid = 0;
        p.stop(Instant::now()); // -> Stopping, administratively_stopped = true
        p.on_reap(exited(0), Instant::now()); // -> Stopped (not Exited)
        assert_eq!(p.state, ProcessState::Stopped);
        // Stopped processes are not respawned by transition (only Exited are).
        p.transition(Instant::now(), false);
        assert_eq!(p.state, ProcessState::Stopped);
    }

    #[test]
    fn event_buffer_overflows_oldest_first() {
        let mut p = make_listener("buffer_size=10\nevents=PROCESS_STATE");
        for serial in 0..12u64 {
            p.buffer_event(serial, "PROCESS_STATE_RUNNING", "x");
        }
        assert_eq!(p.event_buffer.len(), 10);
        // The two oldest (serials 0 and 1) were discarded.
        assert_eq!(p.peek_event().unwrap().0, 2);
    }

    #[test]
    fn listener_subscription_matching() {
        let p = make_listener("events=PROCESS_STATE,TICK_60");
        assert!(p.subscribed_to("PROCESS_STATE_RUNNING"));
        assert!(p.subscribed_to("TICK_60"));
        assert!(!p.subscribed_to("TICK_5"));
        assert!(!p.subscribed_to("PROCESS_COMMUNICATION_STDOUT"));
    }

    #[test]
    fn autorestart_throttles_a_flapping_process() {
        // A startsecs=0 autorestart=true program that exits instantly must not
        // be respawned in the same sub-second window (which would busy-loop).
        let mut p = make_proc("autorestart=true\nstartsecs=0");
        p.state = ProcessState::Exited;
        p.exitstatus = Some(0);
        p.laststart = Some(Instant::now()); // started "just now"
        p.transition(Instant::now(), false);
        // No fork happened: still Exited with no pid.
        assert_eq!(p.state, ProcessState::Exited);
        assert_eq!(p.pid, 0);
    }

    #[test]
    fn write_stdin_buffers_and_respects_its_cap() {
        let (read, write) = make_stdin_pipe().unwrap();
        unsafe {
            let fl = libc::fcntl(read.as_raw_fd(), libc::F_GETFL);
            libc::fcntl(read.as_raw_fd(), libc::F_SETFL, fl | libc::O_NONBLOCK);
        }
        let mut p = make_proc("");
        p.stdin_write = Some(write);

        // A write larger than the pipe buffer is accepted in full and buffered.
        let big = vec![b'y'; 200_000];
        assert_eq!(p.write_stdin(&big), Some(big.len()));

        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        for _ in 0..1000 {
            p.pump_stdin();
            loop {
                let n = unsafe {
                    libc::read(read.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                };
                if n > 0 {
                    got.extend_from_slice(&buf[..n as usize]);
                } else {
                    break;
                }
            }
            if !p.has_pending_write() {
                break;
            }
        }
        assert_eq!(got.len(), big.len());
        assert!(got.iter().all(|&b| b == b'y'));

        // Exceeding the 1 MiB buffer cap is refused rather than growing without
        // bound.
        let toobig = vec![0u8; Process::MAX_STDIN_BUFFER + 1];
        assert_eq!(p.write_stdin(&toobig), None);
    }

    #[test]
    fn write_stdin_without_pipe_returns_none() {
        let mut p = make_proc("");
        assert_eq!(p.write_stdin(b"data"), None);
    }

    #[test]
    fn partial_writes_deliver_a_large_envelope_intact() {
        // Regression test for the partial-write fix: a payload larger than the
        // pipe buffer must be delivered across several pump_stdin() calls with
        // no corruption and no re-sending.
        let (read, write) = make_stdin_pipe().unwrap();
        // Make the read end non-blocking so draining never deadlocks.
        unsafe {
            let fl = libc::fcntl(read.as_raw_fd(), libc::F_GETFL);
            libc::fcntl(read.as_raw_fd(), libc::F_SETFL, fl | libc::O_NONBLOCK);
        }
        let mut p = make_listener("events=PROCESS_STATE");
        p.stdin_write = Some(write);

        let payload = vec![b'x'; 200_000]; // > 64 KiB default pipe buffer
        p.begin_send_event(payload.clone());
        assert_eq!(p.listener_state, ListenerState::Busy);

        let mut got = Vec::new();
        let mut buf = [0u8; 65536];
        for _ in 0..1000 {
            p.pump_stdin();
            loop {
                let n = unsafe {
                    libc::read(read.as_raw_fd(), buf.as_mut_ptr() as *mut libc::c_void, buf.len())
                };
                if n > 0 {
                    got.extend_from_slice(&buf[..n as usize]);
                } else {
                    break;
                }
            }
            if !p.has_pending_write() {
                break;
            }
        }
        assert!(!p.has_pending_write(), "envelope should be fully flushed");
        assert_eq!(got.len(), payload.len(), "every byte delivered exactly once");
        assert!(got.iter().all(|&b| b == b'x'));
    }

    #[test]
    fn splits_quoted_commands() {
        assert_eq!(
            shell_split("/bin/sh -c 'echo hello world'").unwrap(),
            vec!["/bin/sh", "-c", "echo hello world"]
        );
        assert_eq!(
            shell_split(r#"prog --msg="a b" tail"#).unwrap(),
            vec!["prog", "--msg=a b", "tail"]
        );
    }

    #[test]
    fn decodes_exit_status() {
        // exit code 7: status = 7 << 8
        assert_eq!(decode_status(7 << 8), (7, false));
        // killed by SIGKILL (9)
        assert_eq!(decode_status(9), (-9, true));
    }

    #[test]
    fn capture_scanner_extracts_marked_spans() {
        let mut state = CaptureState::default();
        let mut logger = RotatingLogger::null();
        // Split the markers across two feeds to exercise the holdback path.
        let a = b"plain<!--XSUPERVISOR:BEGIN-->hello wo";
        let b = b"rld<!--XSUPERVISOR:END-->tail";
        let mut got = scan_capture(&mut state, a, 100, &mut logger);
        got.extend(scan_capture(&mut state, b, 100, &mut logger));
        assert_eq!(got.len(), 1);
        assert_eq!(got[0], b"hello world");
        assert!(!state.in_capture);
    }

    #[test]
    fn prefix_overlap_detects_split_tokens() {
        assert_eq!(prefix_overlap(b"abc<!--", b"<!--XSUPERVISOR:BEGIN-->"), 4);
        assert_eq!(prefix_overlap(b"abcdef", b"<!--XSUPERVISOR:BEGIN-->"), 0);
    }

    #[test]
    fn formats_durations() {
        assert_eq!(fmt_duration(Duration::from_secs(3661)), "1:01:01");
        assert_eq!(fmt_duration(Duration::from_secs(0)), "0:00:00");
    }
}
