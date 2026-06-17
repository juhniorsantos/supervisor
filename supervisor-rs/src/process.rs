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
use std::os::fd::{AsRawFd, OwnedFd};
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
}

impl Process {
    /// Build a process from its configuration, resolving `AUTO` log paths
    /// against `childlogdir`.
    pub fn new(config: ProgramConfig, childlogdir: &std::path::Path) -> Self {
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
        }
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
        cmd.stdin(Stdio::null());

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
                self.change_state(ProcessState::Starting);
            }
            Err(e) => {
                self.spawnerr = Some(format!("spawn failed: {e}"));
                self.fail_to_backoff(now);
            }
        }
    }

    fn change_state(&mut self, new: ProcessState) {
        self.state = new;
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
        self.pid = 0;
        self.laststop_sys = Some(SystemTime::now());

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
                let _ = expected; // captured for description; restart decided in transition()
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
    /// (non-blocking) and append it to the rotating loggers.
    pub fn drain_output(&mut self) {
        if let Some(fd) = self.stdout_read.as_ref() {
            drain_fd(fd.as_raw_fd(), &mut self.stdout_logger);
        }
        if let Some(fd) = self.stderr_read.as_ref() {
            drain_fd(fd.as_raw_fd(), &mut self.stderr_logger);
        }
    }

    /// One-line status description, mirroring `supervisorctl status`.
    pub fn status_description(&self, now: Instant) -> String {
        match self.state {
            ProcessState::Running => {
                let up = self
                    .laststart
                    .map(|t| now.duration_since(t))
                    .unwrap_or_default();
                format!("pid {}, uptime {}", self.pid, fmt_duration(up))
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
            group: self.config.name.clone(), // each program is its own group
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
    fn formats_durations() {
        assert_eq!(fmt_duration(Duration::from_secs(3661)), "1:01:01");
        assert_eq!(fmt_duration(Duration::from_secs(0)), "0:00:00");
    }
}
