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
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use crate::config::{Config, HttpAuth};
use crate::logger::RotatingLogger;
use crate::process::{Process, ProcessInfo};
use crate::states::SupervisorState;
use crate::{rpc, web};

/// Set from the SIGTERM/SIGINT handler, or the `shutdown` RPC, to request a
/// clean shutdown.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);
/// Set by the `restart` RPC: shut down, then re-exec this binary.
static RESTART_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_term(_sig: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGTERM, handle_term as *const () as usize);
        libc::signal(libc::SIGINT, handle_term as *const () as usize);
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
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

        Ok(Supervisor {
            config,
            processes,
            pid_index: HashMap::new(),
            log,
            socket_path,
            listener,
            inet_listener,
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

        let mut shutting_down = false;

        loop {
            let now = Instant::now();

            if !shutting_down
                && (SHUTDOWN_REQUESTED.load(Ordering::SeqCst)
                    || RESTART_REQUESTED.load(Ordering::SeqCst))
            {
                shutting_down = true;
                self.log_line("WARN", "stopping all processes");
                for i in 0..self.processes.len() {
                    self.processes[i].stop(now);
                }
            }

            self.reap_children(now);

            for i in 0..self.processes.len() {
                self.processes[i].drain_output();
                let had_pid = self.processes[i].pid;
                self.processes[i].transition(now, shutting_down);
                let new_pid = self.processes[i].pid;
                if new_pid != had_pid && new_pid != 0 {
                    self.pid_index.insert(new_pid, i);
                    let name = self.processes[i].name().to_string();
                    self.log_line("INFO", &format!("spawned: '{name}' with pid {new_pid}"));
                }
            }

            self.accept_connections(now);

            if shutting_down && self.all_stopped() {
                break;
            }

            std::thread::sleep(Duration::from_millis(100));
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
                let desc = self.processes[idx].status_description(now);
                self.log_line("INFO", &format!("exited: '{name}' ({desc})"));
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
        if req.method == "POST" {
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
        } else {
            // Web UI (GET).
            let query = path.split_once('?').map(|(_, q)| q).unwrap_or("");
            let html = web::render(self, query, now);
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
