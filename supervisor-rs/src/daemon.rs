//! The `supervisord` event loop and control server.
//!
//! Control happens over HTTP, exactly like the original Supervisor: an
//! XML-RPC endpoint at `POST /RPC2` and a web status UI at `GET /`. Both the
//! unix domain socket and the optional `[inet_http_server]` TCP port are
//! served by the same handler. The XML-RPC method set is compatible enough
//! that the original Python `supervisorctl` can drive this daemon.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::os::unix::net::UnixListener;
use std::path::PathBuf;
use std::os::fd::AsRawFd;
use std::sync::atomic::{AtomicBool, AtomicI32, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::{Config, HttpAuth};
use crate::logger::RotatingLogger;
use crate::process::{Process, ProcessInfo};
use crate::states::SupervisorState;
use crate::{rpc, web};

/// Group names that were `(added, changed, removed)` by a config reread.
pub type ConfigDiff = (Vec<String>, Vec<String>, Vec<String>);

/// Set from the SIGTERM/SIGINT handler, or the `shutdown` RPC, to request a
/// clean shutdown.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
/// Set by the `restart` RPC: shut down, then re-exec this binary.
static RESTART_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_term(_sig: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Write end of a self-pipe; the SIGCHLD handler pokes it so `poll` wakes
/// immediately when a child exits. `-1` until the loop installs it.
static SIGCHLD_PIPE_WRITE: AtomicI32 = AtomicI32::new(-1);

extern "C" fn handle_sigchld(_sig: i32) {
    let fd = SIGCHLD_PIPE_WRITE.load(Ordering::Relaxed);
    if fd >= 0 {
        // write() is async-signal-safe; a single byte is enough to wake poll.
        let byte = [1u8];
        unsafe {
            libc::write(fd, byte.as_ptr() as *const libc::c_void, 1);
        }
    }
}

fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGTERM, handle_term as *const () as usize);
        libc::signal(libc::SIGINT, handle_term as *const () as usize);
        libc::signal(libc::SIGCHLD, handle_sigchld as *const () as usize);
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

/// Block until a control connection arrives, a child exits, or `timeout_ms`
/// elapses — whichever comes first. Replaces a blind sleep so control and
/// reaping are near-instant while timers still tick at the timeout cadence.
fn poll_wait(unix_fd: i32, inet_fd: Option<i32>, sigchld_fd: i32, timeout_ms: i32) {
    let mut fds = vec![
        libc::pollfd {
            fd: unix_fd,
            events: libc::POLLIN,
            revents: 0,
        },
        libc::pollfd {
            fd: sigchld_fd,
            events: libc::POLLIN,
            revents: 0,
        },
    ];
    if let Some(fd) = inet_fd {
        fds.push(libc::pollfd {
            fd,
            events: libc::POLLIN,
            revents: 0,
        });
    }
    let rc = unsafe { libc::poll(fds.as_mut_ptr(), fds.len() as libc::nfds_t, timeout_ms) };
    if rc > 0 {
        // Drain the self-pipe so it doesn't stay readable.
        let mut buf = [0u8; 64];
        loop {
            let n =
                unsafe { libc::read(sigchld_fd, buf.as_mut_ptr() as *mut libc::c_void, buf.len()) };
            if n <= 0 {
                break;
            }
        }
    }
}

/// Create the close-on-exec, non-blocking self-pipe used to wake `poll` on
/// SIGCHLD. Returns `(read_fd, write_fd)`.
fn make_self_pipe() -> (i32, i32) {
    let mut fds = [0i32; 2];
    let rc = unsafe { libc::pipe2(fds.as_mut_ptr(), libc::O_CLOEXEC | libc::O_NONBLOCK) };
    if rc != 0 {
        return (-1, -1);
    }
    (fds[0], fds[1])
}

/// The running supervisor: owns every process and the control endpoints.
pub struct Supervisor {
    config: Config,
    processes: Vec<Process>,
    /// Map of live child pid -> index into `processes`.
    pid_index: HashMap<i32, usize>,
    log: RotatingLogger,
    socket_path: PathBuf,
    listener: UnixListener,
    inet_listener: Option<TcpListener>,
    /// Monotonic global event serial.
    event_serial: u64,
    /// Last emitted tick "slice" for each tick period (epoch-aligned).
    last_tick: [i64; 3],
    /// Path of the loaded config, for `reloadConfig`.
    config_path: Option<PathBuf>,
    /// Group configs known from the most recent (re)read, by group name.
    available_configs: HashMap<String, Vec<crate::config::ProgramConfig>>,
    /// Group configs currently instantiated as live processes.
    active_group_configs: HashMap<String, Vec<crate::config::ProgramConfig>>,
}

impl Supervisor {
    /// Build the supervisor from a parsed config: open the main log, bind the
    /// control endpoints and create one [`Process`] per program instance.
    pub fn new(config: Config) -> Result<Supervisor, String> {
        let _ = std::fs::create_dir_all(&config.supervisord.childlogdir);

        let log = RotatingLogger::new(
            Some(config.supervisord.logfile.clone()),
            config.supervisord.logfile_maxbytes,
            config.supervisord.logfile_backups,
        )
        .map_err(|e| format!("cannot open logfile: {e}"))?;

        let socket_path = config
            .socket_path
            .clone()
            .unwrap_or_else(|| PathBuf::from("/tmp/supervisor.sock"));

        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)
            .map_err(|e| format!("cannot bind control socket {}: {e}", socket_path.display()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("cannot set socket non-blocking: {e}"))?;

        let inet_listener = match &config.inet_addr {
            Some(addr) => {
                let l = TcpListener::bind(addr)
                    .map_err(|e| format!("cannot bind inet server {addr}: {e}"))?;
                l.set_nonblocking(true)
                    .map_err(|e| format!("cannot set inet socket non-blocking: {e}"))?;
                Some(l)
            }
            None => None,
        };

        let childlogdir = config.supervisord.childlogdir.clone();
        let processes = config
            .programs
            .iter()
            .cloned()
            .map(|pc| Process::new(pc, &childlogdir))
            .collect();

        let grouped = group_configs(&config.programs);
        let config_path = config.path.clone();

        Ok(Supervisor {
            config,
            processes,
            pid_index: HashMap::new(),
            log,
            socket_path,
            listener,
            inet_listener,
            event_serial: 0,
            last_tick: [0; 3],
            config_path,
            available_configs: grouped.clone(),
            active_group_configs: grouped,
        })
    }

    /// Write a timestamped line to the main supervisor log.
    fn log_line(&mut self, level: &str, msg: &str) {
        let line = format!("{} {} {}\n", now_timestamp(), level, msg);
        self.log.write(line.as_bytes());
        if !self.config.supervisord.silent && self.config.supervisord.nodaemon {
            print!("{line}");
            let _ = std::io::stdout().flush();
        }
    }

    /// Run the main loop until shutdown (or restart). Blocks for the lifetime
    /// of the daemon.
    pub fn run(&mut self) {
        install_signal_handlers();

        // Self-pipe so SIGCHLD wakes poll() promptly for reaping.
        let (sigchld_read, sigchld_write) = make_self_pipe();
        SIGCHLD_PIPE_WRITE.store(sigchld_write, Ordering::SeqCst);
        let unix_fd = self.listener.as_raw_fd();
        let inet_fd = self.inet_listener.as_ref().map(|l| l.as_raw_fd());

        self.log_line(
            "INFO",
            &format!("supervisord started with pid {}", std::process::id()),
        );

        // Autostart, in priority order (programs are pre-sorted).
        let now = Instant::now();
        for i in 0..self.processes.len() {
            if self.processes[i].config.autostart {
                self.processes[i].start(now);
                if self.processes[i].pid != 0 {
                    self.pid_index.insert(self.processes[i].pid, i);
                    let name = self.processes[i].name().to_string();
                    let pid = self.processes[i].pid;
                    self.log_line("INFO", &format!("spawned: '{name}' with pid {pid}"));
                }
            }
        }

        // Align tick slices to "now" so we don't fire ticks at boot.
        let boot = self.now_epoch();
        self.last_tick = [boot - boot % 5, boot - boot % 60, boot - boot % 3600];
        // Announce that the supervisor is running.
        self.emit_event("SUPERVISOR_STATE_CHANGE_RUNNING", String::new());

        let mut shutting_down = false;

        loop {
            let now = Instant::now();

            if !shutting_down
                && (SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
                    || RESTART_REQUESTED.load(Ordering::SeqCst))
            {
                shutting_down = true;
                self.log_line("WARN", "stopping all processes");
                self.emit_event("SUPERVISOR_STATE_CHANGE_STOPPING", String::new());
                for i in 0..self.processes.len() {
                    self.processes[i].stop(now);
                }
            }

            self.reap_children(now);

            for i in 0..self.processes.len() {
                self.processes[i].drain_output();
                // Drain any queued stdin (sendProcessStdin) toward the child.
                self.processes[i].pump_stdin();
                let had_pid = self.processes[i].pid;
                self.processes[i].transition(now, shutting_down);
                let new_pid = self.processes[i].pid;
                if new_pid != had_pid && new_pid != 0 {
                    self.pid_index.insert(new_pid, i);
                    let name = self.processes[i].name().to_string();
                    self.log_line("INFO", &format!("spawned: '{name}' with pid {new_pid}"));
                }
            }

            // Route process-state and tick events to listeners, then deliver
            // buffered events to any ready listener.
            self.route_events();
            self.dispatch_to_listeners();

            self.accept_connections(now);

            if shutting_down && self.all_stopped() {
                break;
            }

            // Wait for the next control connection, child exit, or 100 ms
            // timer tick — whichever is first.
            poll_wait(unix_fd, inet_fd, sigchld_read, 100);
        }

        SIGCHLD_PIPE_WRITE.store(-1, Ordering::SeqCst);
        if sigchld_read >= 0 {
            unsafe {
                libc::close(sigchld_read);
                libc::close(sigchld_write);
            }
        }

        self.log_line("INFO", "supervisord stopped");
        self.cleanup();

        if RESTART_REQUESTED.load(Ordering::SeqCst) {
            self.exec_self();
        }
    }

    fn all_stopped(&self) -> bool {
        self.processes
            .iter()
            .all(|p| p.pid == 0 && p.state.is_stopped())
    }

    /// Reap exited children with `waitpid(WNOHANG)` and notify their process.
    fn reap_children(&mut self, now: Instant) {
        loop {
            let mut status: i32 = 0;
            let pid = unsafe { libc::waitpid(-1, &mut status, libc::WNOHANG) };
            if pid <= 0 {
                break;
            }
            if let Some(&idx) = self.pid_index.get(&pid) {
                self.pid_index.remove(&pid);
                self.processes[idx].on_reap(status, now);
                let name = self.processes[idx].name().to_string();
                let desc = self.processes[idx].status_description(self.now_epoch());
                self.log_line("INFO", &format!("exited: '{name}' ({desc})"));
            }
        }
    }

    // -- Event subsystem ---------------------------------------------------

    /// Collect process-state events produced this tick, generate any due tick
    /// events, and buffer them into every subscribed listener.
    fn route_events(&mut self) {
        // Drain per-process state-change events (preserving order).
        let mut events: Vec<(String, String)> = Vec::new();
        for p in &mut self.processes {
            if !p.pending_events.is_empty() {
                events.append(&mut p.pending_events);
            }
        }

        // Generate tick events on period boundaries.
        let now = self.now_epoch();
        for (idx, &(period, name)) in
            [(5i64, "TICK_5"), (60, "TICK_60"), (3600, "TICK_3600")].iter().enumerate()
        {
            let slice = now - now % period;
            if self.last_tick[idx] != slice {
                self.last_tick[idx] = slice;
                events.push((name.to_string(), format!("when:{now}")));
            }
        }

        for (name, payload) in events {
            self.buffer_to_listeners(&name, payload);
        }
    }

    /// Assign a serial to an event and buffer it into every subscribed
    /// listener pool.
    fn buffer_to_listeners(&mut self, name: &str, payload: String) {
        let serial = self.event_serial;
        self.event_serial += 1;
        for p in &mut self.processes {
            if p.is_listener() && p.subscribed_to(name) {
                p.buffer_event(serial, name, &payload);
            }
        }
    }

    /// A convenience for supervisor-level events (no per-process source).
    fn emit_event(&mut self, name: &str, payload: String) {
        self.buffer_to_listeners(name, payload);
    }

    /// Continue any in-flight envelope writes, then hand the oldest buffered
    /// event to each ready listener.
    fn dispatch_to_listeners(&mut self) {
        let identifier = self.config.supervisord.identifier.clone();
        for p in &mut self.processes {
            if !p.is_listener() {
                continue;
            }
            // Finish flushing a previous (partial) envelope before sending more.
            if p.has_pending_write() {
                p.pump_stdin();
                continue;
            }
            if !p.listener_ready() {
                continue;
            }
            if let Some((serial, name, payload)) = p.peek_event() {
                let poolserial = p.pool_serial;
                p.pool_serial += 1;
                let envelope = format!(
                    "ver:3.0 server:{identifier} serial:{serial} pool:{pool} \
                     poolserial:{poolserial} eventname:{name} len:{len}\n{payload}",
                    pool = p.pool_name(),
                    len = payload.len(),
                );
                p.begin_send_event(envelope.into_bytes());
            }
        }
    }

    // -- Control server ----------------------------------------------------

    fn accept_connections(&mut self, now: Instant) {
        // Unix socket connections.
        loop {
            match self.listener.accept() {
                Ok((mut s, _)) => {
                    let auth = self.config.unix_auth.clone();
                    let _ = s.set_nonblocking(false);
                    let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
                    self.serve_http(&mut s, auth, now);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
        // Inet (TCP) connections.
        if self.inet_listener.is_some() {
            loop {
                let accepted = self.inet_listener.as_ref().unwrap().accept();
                match accepted {
                    Ok((mut s, _)) => {
                        let auth = self.config.inet_auth.clone();
                        let _ = s.set_nonblocking(false);
                        let _ = s.set_read_timeout(Some(Duration::from_secs(5)));
                        self.serve_http(&mut s, auth, now);
                    }
                    Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                    Err(_) => break,
                }
            }
        }
    }

    /// Read and respond to a single HTTP request on `stream`.
    fn serve_http<S: Read + Write>(&mut self, stream: &mut S, auth: HttpAuth, now: Instant) {
        let req = match crate::http::read_request(stream) {
            Some(r) => r,
            None => return,
        };

        // Optional HTTP Basic auth.
        if auth.is_set() {
            let ok = req
                .basic_auth()
                .map(|(u, p)| {
                    Some(u) == auth.username
                        && (auth.password.is_none() || Some(p) == auth.password)
                })
                .unwrap_or(false);
            if !ok {
                crate::http::write_response(
                    stream,
                    401,
                    "Unauthorized",
                    "text/plain",
                    "401 Unauthorized\n",
                    &[("WWW-Authenticate", "Basic realm=\"supervisor\"")],
                );
                return;
            }
        }

        let path = req.path.clone();
        let route = path.split_once('?').map(|(p, _)| p).unwrap_or(&path);

        if req.method == "POST" && route == "/RPC2" {
            // XML-RPC endpoint.
            match crate::xmlrpc::parse_method_call(&req.body) {
                Ok((method, params)) => {
                    let body = match rpc::dispatch(self, &method, &params, now) {
                        Ok(value) => crate::xmlrpc::serialize_response(&value),
                        Err((code, msg)) => crate::xmlrpc::serialize_fault(code, &msg),
                    };
                    crate::http::write_response(stream, 200, "OK", "text/xml", &body, &[]);
                }
                Err(e) => {
                    let body = crate::xmlrpc::serialize_fault(1, &format!("malformed call: {e}"));
                    crate::http::write_response(stream, 200, "OK", "text/xml", &body, &[]);
                }
            }
        } else if req.method == "POST" {
            // Web UI action submitted as a form POST. Actions are never taken
            // on a GET, so the page is safe to refresh/prefetch.
            let params = web::parse_form(&req.body);
            let html = web::render(self, &params, now);
            crate::http::write_response(stream, 200, "OK", "text/html; charset=utf-8", &html, &[]);
        } else {
            // Web UI (GET): render status with no side effects.
            let html = web::render(self, &[], now);
            crate::http::write_response(stream, 200, "OK", "text/html; charset=utf-8", &html, &[]);
        }
    }

    // -- Operations used by the RPC and web layers -------------------------

    pub fn identifier(&self) -> &str {
        &self.config.supervisord.identifier
    }

    pub fn supervisor_state(&self) -> SupervisorState {
        if SHUTDOWN_REQUESTED.load(Ordering::SeqCst) || RESTART_REQUESTED.load(Ordering::SeqCst) {
            SupervisorState::Shutdown
        } else {
            SupervisorState::Running
        }
    }

    pub fn now_epoch(&self) -> i64 {
        SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .map(|d| d.as_secs() as i64)
            .unwrap_or(0)
    }

    /// Snapshot of every process, in config order.
    pub fn all_process_info(&self) -> Vec<ProcessInfo> {
        let now = self.now_epoch();
        self.processes.iter().map(|p| p.info(now)).collect()
    }

    /// Snapshot of a single process by name.
    pub fn process_info(&self, name: &str) -> Option<ProcessInfo> {
        let now = self.now_epoch();
        self.find(name).map(|i| self.processes[i].info(now))
    }

    pub fn supervisor_pid(&self) -> i32 {
        std::process::id() as i32
    }

    pub fn request_shutdown(&self) {
        SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
    }

    pub fn request_restart(&self) {
        RESTART_REQUESTED.store(true, Ordering::SeqCst);
    }

    /// Start a process by name. Returns a `(fault_code, message)` on error.
    pub fn op_start(&mut self, name: &str, now: Instant) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        if self.processes[idx].state.is_running() {
            return Err((rpc::faults::ALREADY_STARTED, name.to_string()));
        }
        self.processes[idx].start(now);
        self.resync_pid_index();
        // Reap immediately so an instant spawn failure is reflected now.
        self.reap_children(now);
        self.processes[idx].transition(now, false);
        if self.processes[idx].spawnerr.is_some() && !self.processes[idx].state.is_running() {
            return Err((rpc::faults::SPAWN_ERROR, name.to_string()));
        }
        Ok(())
    }

    /// Stop a process by name. Returns a `(fault_code, message)` on error.
    pub fn op_stop(&mut self, name: &str, now: Instant) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        if !self.processes[idx].state.is_running() {
            return Err((rpc::faults::NOT_RUNNING, name.to_string()));
        }
        self.processes[idx].stop(now);
        self.reap_children(now);
        Ok(())
    }

    /// Restart a process by name (stop, then auto re-spawn on exit).
    pub fn op_restart(&mut self, name: &str, now: Instant) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        self.processes[idx].request_restart(now);
        self.resync_pid_index();
        Ok(())
    }

    /// Start every process; returns one `(name, group, status, description)`
    /// per process, mirroring `startAllProcesses`.
    pub fn op_start_all(&mut self, now: Instant) -> Vec<(String, String, i32, String)> {
        let names: Vec<String> = self.processes.iter().map(|p| p.name().to_string()).collect();
        let mut out = Vec::new();
        for name in names {
            let (code, desc) = match self.op_start(&name, now) {
                Ok(()) => (rpc::faults::SUCCESS, "started".to_string()),
                Err((c, _)) => (c, fault_message(c)),
            };
            out.push((name.clone(), name, code, desc));
        }
        out
    }

    /// Stop every process; mirrors `stopAllProcesses`.
    pub fn op_stop_all(&mut self, now: Instant) -> Vec<(String, String, i32, String)> {
        let names: Vec<String> = self.processes.iter().map(|p| p.name().to_string()).collect();
        let mut out = Vec::new();
        for name in names {
            let (code, desc) = match self.op_stop(&name, now) {
                Ok(()) => (rpc::faults::SUCCESS, "stopped".to_string()),
                Err((c, _)) => (c, fault_message(c)),
            };
            out.push((name.clone(), name, code, desc));
        }
        out
    }

    /// Start every process in a group; mirrors `startProcessGroup`.
    pub fn op_start_group(&mut self, group: &str, now: Instant) -> Vec<(String, String, i32, String)> {
        let names: Vec<String> = self
            .processes
            .iter()
            .filter(|p| p.config.group == group)
            .map(|p| p.name().to_string())
            .collect();
        let mut out = Vec::new();
        for name in names {
            let (code, desc) = match self.op_start(&name, now) {
                Ok(()) => (rpc::faults::SUCCESS, "started".to_string()),
                Err((c, _)) => (c, fault_message(c)),
            };
            out.push((name, group.to_string(), code, desc));
        }
        out
    }

    /// Stop every process in a group; mirrors `stopProcessGroup`.
    pub fn op_stop_group(&mut self, group: &str, now: Instant) -> Vec<(String, String, i32, String)> {
        let names: Vec<String> = self
            .processes
            .iter()
            .filter(|p| p.config.group == group)
            .map(|p| p.name().to_string())
            .collect();
        let mut out = Vec::new();
        for name in names {
            let (code, desc) = match self.op_stop(&name, now) {
                Ok(()) => (rpc::faults::SUCCESS, "stopped".to_string()),
                Err((c, _)) => (c, fault_message(c)),
            };
            out.push((name, group.to_string(), code, desc));
        }
        out
    }

    /// Whether `group` is a currently-active process group.
    pub fn has_group(&self, group: &str) -> bool {
        self.active_group_configs.contains_key(group)
    }

    /// Re-read the configuration file and report `(added, changed, removed)`
    /// group names relative to the active set. Does not apply the changes;
    /// `add_process_group`/`remove_process_group` do that.
    pub fn reload_config(&mut self) -> Result<ConfigDiff, (i32, String)> {
        let path = self
            .config_path
            .clone()
            .ok_or((rpc::faults::CANT_REREAD, "no config file path".to_string()))?;
        let new_config = Config::load(&path).map_err(|e| (rpc::faults::CANT_REREAD, e))?;
        let new_groups = group_configs(&new_config.programs);

        let mut added = Vec::new();
        let mut changed = Vec::new();
        let mut removed = Vec::new();
        for (name, cfgs) in &new_groups {
            match self.active_group_configs.get(name) {
                None => added.push(name.clone()),
                Some(active) if active != cfgs => changed.push(name.clone()),
                Some(_) => {}
            }
        }
        for name in self.active_group_configs.keys() {
            if !new_groups.contains_key(name) {
                removed.push(name.clone());
            }
        }
        added.sort();
        changed.sort();
        removed.sort();

        // The newly-read groups become available for add/update.
        self.available_configs = new_groups;
        Ok((added, changed, removed))
    }

    /// Instantiate and activate the group `name` from the last reread config.
    pub fn add_process_group(&mut self, name: &str, now: Instant) -> Result<(), (i32, String)> {
        if self.active_group_configs.contains_key(name) {
            return Err((rpc::faults::ALREADY_ADDED, name.to_string()));
        }
        let cfgs = self
            .available_configs
            .get(name)
            .cloned()
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;

        let childlogdir = self.config.supervisord.childlogdir.clone();
        for cfg in &cfgs {
            let mut proc = Process::new(cfg.clone(), &childlogdir);
            if cfg.autostart {
                proc.start(now);
            }
            self.processes.push(proc);
        }
        self.active_group_configs.insert(name.to_string(), cfgs);
        self.resync_pid_index();
        self.log_line("INFO", &format!("added process group '{name}'"));
        Ok(())
    }

    /// Remove the group `name`; fails if any of its processes are running.
    pub fn remove_process_group(&mut self, name: &str) -> Result<(), (i32, String)> {
        if !self.active_group_configs.contains_key(name) {
            return Err((rpc::faults::BAD_NAME, name.to_string()));
        }
        let still_running = self
            .processes
            .iter()
            .any(|p| p.config.group == name && (p.pid != 0 || !p.state.is_stopped()));
        if still_running {
            return Err((rpc::faults::STILL_RUNNING, name.to_string()));
        }
        self.processes.retain(|p| p.config.group != name);
        self.active_group_configs.remove(name);
        self.resync_pid_index();
        self.log_line("INFO", &format!("removed process group '{name}'"));
        Ok(())
    }

    /// Read up to `length` bytes from a process log starting at `offset`.
    /// `channel` is "stdout" or "stderr". Negative offsets are unsupported.
    pub fn read_log(
        &self,
        name: &str,
        channel: &str,
        offset: i64,
        length: i64,
    ) -> Result<String, (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        let path = match channel {
            "stderr" => &self.processes[idx].stderr_path,
            _ => &self.processes[idx].stdout_path,
        };
        if path.is_empty() || !std::path::Path::new(path).exists() {
            return Err((rpc::faults::NO_FILE, path.clone()));
        }
        let data = std::fs::read(path).map_err(|e| (rpc::faults::FAILED, e.to_string()))?;
        let start = offset.max(0) as usize;
        if start >= data.len() {
            return Ok(String::new());
        }
        let slice = if length <= 0 {
            &data[start..]
        } else {
            let end = (start + length as usize).min(data.len());
            &data[start..end]
        };
        Ok(String::from_utf8_lossy(slice).into_owned())
    }

    /// Read up to `length` bytes of the main supervisord log from `offset`.
    pub fn read_main_log(&self, offset: i64, length: i64) -> Result<String, (i32, String)> {
        let path = &self.config.supervisord.logfile;
        if !path.exists() {
            return Err((rpc::faults::NO_FILE, path.display().to_string()));
        }
        let data = std::fs::read(path).map_err(|e| (rpc::faults::FAILED, e.to_string()))?;
        let start = (offset.max(0) as usize).min(data.len());
        let slice = if length <= 0 {
            &data[start..]
        } else {
            let end = (start + length as usize).min(data.len());
            &data[start..end]
        };
        Ok(String::from_utf8_lossy(slice).into_owned())
    }

    /// Clear (truncate) the main supervisord log.
    pub fn clear_main_log(&mut self) -> Result<(), (i32, String)> {
        self.log.clear();
        Ok(())
    }

    /// Send a UNIX signal to a process by name.
    pub fn op_signal(&mut self, name: &str, sig: i32) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        if !self.processes[idx].signal(sig) {
            return Err((rpc::faults::NOT_RUNNING, name.to_string()));
        }
        Ok(())
    }

    /// Signal every process in a group.
    pub fn op_signal_group(&mut self, group: &str, sig: i32) -> Vec<(String, String, i32, String)> {
        let names: Vec<String> = self
            .processes
            .iter()
            .filter(|p| p.config.group == group)
            .map(|p| p.name().to_string())
            .collect();
        names
            .into_iter()
            .map(|name| match self.op_signal(&name, sig) {
                Ok(()) => (name, group.to_string(), rpc::faults::SUCCESS, "signalled".into()),
                Err((c, _)) => (name, group.to_string(), c, fault_message(c)),
            })
            .collect()
    }

    /// Signal every process.
    pub fn op_signal_all(&mut self, sig: i32) -> Vec<(String, String, i32, String)> {
        let entries: Vec<(String, String)> = self
            .processes
            .iter()
            .map(|p| (p.name().to_string(), p.config.group.clone()))
            .collect();
        entries
            .into_iter()
            .map(|(name, group)| match self.op_signal(&name, sig) {
                Ok(()) => (name, group, rpc::faults::SUCCESS, "signalled".into()),
                Err((c, _)) => (name, group, c, fault_message(c)),
            })
            .collect()
    }

    /// Truncate a process's stdout/stderr logs.
    pub fn op_clear_logs(&mut self, name: &str) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        self.processes[idx].clear_logs();
        Ok(())
    }

    /// Truncate every process's logs.
    pub fn op_clear_all_logs(&mut self) -> Vec<(String, String, i32, String)> {
        let mut out = Vec::new();
        for p in &mut self.processes {
            p.clear_logs();
            out.push((
                p.name().to_string(),
                p.config.group.clone(),
                rpc::faults::SUCCESS,
                "cleared".to_string(),
            ));
        }
        out
    }

    /// Write `data` to a process's stdin.
    pub fn op_send_stdin(&mut self, name: &str, data: &str) -> Result<(), (i32, String)> {
        let idx = self
            .find(name)
            .ok_or((rpc::faults::BAD_NAME, name.to_string()))?;
        if self.processes[idx].state != crate::states::ProcessState::Running {
            return Err((rpc::faults::NOT_RUNNING, name.to_string()));
        }
        match self.processes[idx].write_stdin(data.as_bytes()) {
            Some(_) => Ok(()),
            None => Err((rpc::faults::NO_FILE, name.to_string())),
        }
    }

    /// Emit a REMOTE_COMMUNICATION event into the listener subsystem.
    pub fn op_send_remote_comm_event(&mut self, kind: &str, data: &str) {
        let payload = format!("type:{kind}\n{data}");
        self.buffer_to_listeners("REMOTE_COMMUNICATION", payload);
    }

    /// Find a process index by its name (also accepts the `group:name`
    /// namespec where group == name).
    fn find(&self, name: &str) -> Option<usize> {
        let short = name.split_once(':').map(|(_, p)| p).unwrap_or(name);
        self.processes
            .iter()
            .position(|p| p.name() == name || p.name() == short)
    }

    fn resync_pid_index(&mut self) {
        self.pid_index.clear();
        for (i, p) in self.processes.iter().enumerate() {
            if p.pid != 0 {
                self.pid_index.insert(p.pid, i);
            }
        }
    }

    fn cleanup(&mut self) {
        let _ = std::fs::remove_file(&self.socket_path);
        let _ = std::fs::remove_file(&self.config.supervisord.pidfile);
    }

    /// Re-exec this binary with the original arguments (for `restart`).
    fn exec_self(&mut self) {
        use std::os::unix::process::CommandExt;
        RESTART_REQUESTED.store(false, Ordering::SeqCst);
        SHUTDOWN_REQUESTED.store(false, Ordering::SeqCst);
        let args: Vec<String> = std::env::args().collect();
        if let Ok(exe) = std::env::current_exe() {
            let err = std::process::Command::new(exe).args(&args[1..]).exec();
            self.log_line("ERRO", &format!("re-exec failed: {err}"));
        }
    }
}

/// Group a flat list of program configs by their group name, preserving
/// each group's program order.
fn group_configs(
    programs: &[crate::config::ProgramConfig],
) -> HashMap<String, Vec<crate::config::ProgramConfig>> {
    let mut map: HashMap<String, Vec<crate::config::ProgramConfig>> = HashMap::new();
    for p in programs {
        map.entry(p.group.clone()).or_default().push(p.clone());
    }
    map
}

/// Map a fault code to the short message the web UI shows.
fn fault_message(code: i32) -> String {
    match code {
        rpc::faults::ALREADY_STARTED => "already started".to_string(),
        rpc::faults::NOT_RUNNING => "not running".to_string(),
        rpc::faults::SPAWN_ERROR => "spawn error".to_string(),
        rpc::faults::BAD_NAME => "no such process".to_string(),
        _ => "error".to_string(),
    }
}

/// A short, syslog-ish timestamp for log lines.
fn now_timestamp() -> String {
    let now = unsafe { libc::time(std::ptr::null_mut()) };
    let mut tm: libc::tm = unsafe { std::mem::zeroed() };
    unsafe { libc::localtime_r(&now, &mut tm) };
    format!(
        "{:04}-{:02}-{:02} {:02}:{:02}:{:02}",
        tm.tm_year + 1900,
        tm.tm_mon + 1,
        tm.tm_mday,
        tm.tm_hour,
        tm.tm_min,
        tm.tm_sec
    )
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::xmlrpc::Value;

    /// A unique temp directory for a test (cleaned up at the end).
    fn temp_dir(tag: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("suprs-{}-{}", std::process::id(), tag));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn cfg_text(dir: &std::path::Path, programs: &str) -> String {
        let d = dir.display();
        format!(
            "[unix_http_server]\nfile={d}/s.sock\n\
             [supervisord]\nlogfile={d}/sd.log\nchildlogdir={d}\npidfile={d}/sd.pid\n\
             {programs}"
        )
    }

    fn build(dir: &std::path::Path, programs: &str) -> Supervisor {
        let cfg = Config::parse(&cfg_text(dir, programs)).unwrap();
        Supervisor::new(cfg).unwrap()
    }

    /// Look up a struct field in an XML-RPC value.
    fn field<'a>(v: &'a Value, key: &str) -> Option<&'a Value> {
        if let Value::Struct(members) = v {
            members.iter().find(|(k, _)| k == key).map(|(_, val)| val)
        } else {
            None
        }
    }

    fn call(sup: &mut Supervisor, method: &str, params: &[Value]) -> Result<Value, (i32, String)> {
        rpc::dispatch(sup, method, params, Instant::now())
    }

    #[test]
    fn rpc_basic_introspection() {
        let dir = temp_dir("introspect");
        let mut sup = build(&dir, "[program:web]\ncommand=/bin/true\nautostart=false\n");

        assert_eq!(
            call(&mut sup, "supervisor.getAPIVersion", &[]).unwrap(),
            Value::Str("3.0".into())
        );
        let state = call(&mut sup, "supervisor.getState", &[]).unwrap();
        assert_eq!(field(&state, "statename"), Some(&Value::Str("RUNNING".into())));
        assert_eq!(
            call(&mut sup, "supervisor.getPID", &[]).unwrap(),
            Value::Int(std::process::id() as i64)
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn rpc_process_info_and_faults() {
        let dir = temp_dir("info");
        let mut sup = build(&dir, "[program:web]\ncommand=/bin/true\nautostart=false\n");

        let all = call(&mut sup, "supervisor.getAllProcessInfo", &[]).unwrap();
        match all {
            Value::Array(items) => {
                assert_eq!(items.len(), 1);
                assert_eq!(field(&items[0], "name"), Some(&Value::Str("web".into())));
                // Not started yet -> STOPPED (state code 0).
                assert_eq!(field(&items[0], "state"), Some(&Value::Int(0)));
            }
            other => panic!("expected array, got {other:?}"),
        }

        // Unknown process name -> BAD_NAME fault.
        let err = call(&mut sup, "supervisor.getProcessInfo", &[Value::Str("nope".into())]);
        assert_eq!(err.unwrap_err().0, rpc::faults::BAD_NAME);

        // Unknown method -> UNKNOWN_METHOD fault.
        let err = call(&mut sup, "supervisor.bogusMethod", &[]);
        assert_eq!(err.unwrap_err().0, rpc::faults::UNKNOWN_METHOD);

        // Stopping a process that isn't running -> NOT_RUNNING.
        let err = call(&mut sup, "supervisor.stopProcess", &[Value::Str("web".into())]);
        assert_eq!(err.unwrap_err().0, rpc::faults::NOT_RUNNING);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn signal_and_stdin_faults_on_stopped_process() {
        let dir = temp_dir("signal");
        let mut sup = build(&dir, "[program:web]\ncommand=/bin/true\nautostart=false\n");

        // A stopped process is not signallable / writable.
        assert_eq!(
            sup.op_signal("web", libc::SIGHUP).unwrap_err().0,
            rpc::faults::NOT_RUNNING
        );
        assert_eq!(
            sup.op_send_stdin("web", "hi\n").unwrap_err().0,
            rpc::faults::NOT_RUNNING
        );
        // Bad signal name is rejected before reaching a process.
        let err = call(
            &mut sup,
            "supervisor.signalProcess",
            &[Value::Str("web".into()), Value::Str("NOTASIGNAL".into())],
        );
        assert_eq!(err.unwrap_err().0, rpc::faults::BAD_SIGNAL);

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn groups_are_reported_in_process_info() {
        let dir = temp_dir("groups");
        let sup = build(
            &dir,
            "[program:a]\ncommand=/bin/true\nautostart=false\n\
             [program:b]\ncommand=/bin/true\nautostart=false\n\
             [group:grp]\nprograms=a,b\n",
        );
        let info = sup.process_info("a").unwrap();
        assert_eq!(info.group, "grp");
        // The group:name namespec resolves to the same process.
        assert_eq!(sup.process_info("grp:a").unwrap().name, "a");
        assert!(sup.has_group("grp"));

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reload_config_detects_added_changed_removed() {
        let dir = temp_dir("reload");
        let conf_path = dir.join("supervisord.conf");

        std::fs::write(
            &conf_path,
            cfg_text(&dir, "[program:keep]\ncommand=/bin/true\nautostart=false\n\
                            [program:gone]\ncommand=/bin/true\nautostart=false\n"),
        )
        .unwrap();
        let cfg = Config::load(&conf_path).unwrap();
        let mut sup = Supervisor::new(cfg).unwrap();

        // Rewrite the file: drop `gone`, change `keep`'s command, add `fresh`.
        std::fs::write(
            &conf_path,
            cfg_text(&dir, "[program:keep]\ncommand=/bin/false\nautostart=false\n\
                            [program:fresh]\ncommand=/bin/true\nautostart=false\n"),
        )
        .unwrap();

        let (added, changed, removed) = sup.reload_config().unwrap();
        assert_eq!(added, vec!["fresh".to_string()]);
        assert_eq!(changed, vec!["keep".to_string()]);
        assert_eq!(removed, vec!["gone".to_string()]);

        // Apply: add the new group, then it must exist and be removable.
        sup.add_process_group("fresh", Instant::now()).unwrap();
        assert!(sup.has_group("fresh"));
        // Adding twice -> ALREADY_ADDED.
        assert_eq!(
            sup.add_process_group("fresh", Instant::now()).unwrap_err().0,
            rpc::faults::ALREADY_ADDED
        );
        // It is stopped (autostart=false), so removal succeeds.
        sup.remove_process_group("fresh").unwrap();
        assert!(!sup.has_group("fresh"));
        // Removing an unknown group -> BAD_NAME.
        assert_eq!(
            sup.remove_process_group("nope").unwrap_err().0,
            rpc::faults::BAD_NAME
        );

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn reload_with_no_changes_reports_nothing() {
        let dir = temp_dir("reload-noop");
        let conf_path = dir.join("supervisord.conf");
        let body = cfg_text(
            &dir,
            "[program:a]\ncommand=/bin/true\nnumprocs=2\nprocess_name=%(program_name)s_%(process_num)02d\nautostart=false\n\
             [group:g]\nprograms=a\n",
        );
        std::fs::write(&conf_path, &body).unwrap();
        let cfg = Config::load(&conf_path).unwrap();
        let mut sup = Supervisor::new(cfg).unwrap();

        // Re-reading the identical file must not flag any group as changed,
        // which would otherwise make `update` needlessly restart processes.
        let (added, changed, removed) = sup.reload_config().unwrap();
        assert!(added.is_empty(), "unexpected added: {added:?}");
        assert!(changed.is_empty(), "unexpected changed: {changed:?}");
        assert!(removed.is_empty(), "unexpected removed: {removed:?}");

        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn clear_main_log_truncates() {
        let dir = temp_dir("clearlog");
        let mut sup = build(&dir, "");
        sup.log_line("INFO", "some noise to make the log non-empty");
        let logfile = dir.join("sd.log");
        assert!(std::fs::metadata(&logfile).unwrap().len() > 0);
        sup.clear_main_log().unwrap();
        assert_eq!(std::fs::metadata(&logfile).unwrap().len(), 0);

        let _ = std::fs::remove_dir_all(&dir);
    }
}
