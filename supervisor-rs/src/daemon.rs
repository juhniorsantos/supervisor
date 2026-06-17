//! The `supervisord` event loop, control server and command dispatch.

use std::collections::HashMap;
use std::io::{Read, Write};
use std::os::unix::net::{UnixListener, UnixStream};
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use crate::config::Config;
use crate::logger::RotatingLogger;
use crate::process::Process;

/// Set from the SIGTERM/SIGINT handler to request a clean shutdown.
static SHUTDOWN_REQUESTED: AtomicBool = AtomicBool::new(false);

extern "C" fn handle_term(_sig: i32) {
    SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
}

/// Install signal handlers: graceful shutdown on TERM/INT, ignore SIGPIPE so
/// a disconnected control client can't take the daemon down.
fn install_signal_handlers() {
    unsafe {
        libc::signal(libc::SIGTERM, handle_term as *const () as usize);
        libc::signal(libc::SIGINT, handle_term as *const () as usize);
        libc::signal(libc::SIGPIPE, libc::SIG_IGN);
    }
}

/// The running supervisor: owns every process and the control socket.
pub struct Supervisor {
    config: Config,
    processes: Vec<Process>,
    /// Map of live child pid -> index into `processes`.
    pid_index: HashMap<i32, usize>,
    log: RotatingLogger,
    socket_path: PathBuf,
    listener: UnixListener,
}

impl Supervisor {
    /// Build the supervisor from a parsed config: open the main log, bind the
    /// control socket and create one [`Process`] per program instance.
    pub fn new(config: Config) -> Result<Supervisor, String> {
        // Ensure childlogdir exists.
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

        // Remove a stale socket, then bind.
        let _ = std::fs::remove_file(&socket_path);
        let listener = UnixListener::bind(&socket_path)
            .map_err(|e| format!("cannot bind control socket {}: {e}", socket_path.display()))?;
        listener
            .set_nonblocking(true)
            .map_err(|e| format!("cannot set socket non-blocking: {e}"))?;

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

    /// Run the main loop until a shutdown is requested and all processes have
    /// stopped. This blocks for the lifetime of the daemon.
    pub fn run(&mut self) {
        install_signal_handlers();
        self.log_line(
            "INFO",
            &format!(
                "supervisord started with pid {}",
                std::process::id()
            ),
        );

        // Autostart, in priority order (programs are pre-sorted).
        let now = Instant::now();
        for i in 0..self.processes.len() {
            if self.processes[i].config.autostart {
                self.processes[i].start(now);
                if self.processes[i].pid != 0 {
                    self.pid_index.insert(self.processes[i].pid, i);
                    let name = self.processes[i].name().to_string();
                    self.log_line("INFO", &format!("spawned: '{name}' with pid {}", self.processes[i].pid));
                }
            }
        }

        let mut shutting_down = false;

        loop {
            let now = Instant::now();

            // 1. Begin shutdown if signalled.
            if !shutting_down && SHUTDOWN_REQUESTED.load(Ordering::SeqCst) {
                shutting_down = true;
                self.log_line("WARN", "received shutdown request, stopping processes");
                for i in 0..self.processes.len() {
                    self.processes[i].stop(now);
                }
            }

            // 2. Reap any exited children.
            self.reap_children(now);

            // 3. Drain child output and advance each state machine.
            for i in 0..self.processes.len() {
                self.processes[i].drain_output();
                let had_pid = self.processes[i].pid;
                self.processes[i].transition(now, shutting_down);
                let new_pid = self.processes[i].pid;
                // A transition may have spawned a new pid (backoff retry,
                // autorestart). Keep the index in sync.
                if new_pid != had_pid && new_pid != 0 {
                    self.pid_index.insert(new_pid, i);
                    let name = self.processes[i].name().to_string();
                    self.log_line("INFO", &format!("spawned: '{name}' with pid {new_pid}"));
                }
            }

            // 4. Service control connections.
            self.accept_control(now);

            // 5. Exit once everything is down during shutdown.
            if shutting_down && self.all_stopped() {
                break;
            }

            std::thread::sleep(Duration::from_millis(100));
        }

        self.log_line("INFO", "supervisord stopped");
        self.cleanup();
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
                break; // 0 = no child ready, <0 = no children at all
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

    fn accept_control(&mut self, now: Instant) {
        loop {
            match self.listener.accept() {
                Ok((stream, _addr)) => {
                    self.handle_control_client(stream, now);
                }
                Err(ref e) if e.kind() == std::io::ErrorKind::WouldBlock => break,
                Err(_) => break,
            }
        }
    }

    fn handle_control_client(&mut self, mut stream: UnixStream, now: Instant) {
        let _ = stream.set_nonblocking(false);
        let _ = stream.set_read_timeout(Some(Duration::from_secs(2)));
        let mut buf = Vec::new();
        let mut chunk = [0u8; 1024];
        // Read until newline or EOF.
        loop {
            match stream.read(&mut chunk) {
                Ok(0) => break,
                Ok(n) => {
                    buf.extend_from_slice(&chunk[..n]);
                    if buf.contains(&b'\n') {
                        break;
                    }
                }
                Err(_) => break,
            }
        }
        let line = String::from_utf8_lossy(&buf);
        let command = line.lines().next().unwrap_or("").trim().to_string();
        let response = self.dispatch(&command, now);
        let _ = stream.write_all(response.as_bytes());
        let _ = stream.flush();
    }

    /// Execute a control command and return the textual response.
    fn dispatch(&mut self, command: &str, now: Instant) -> String {
        let mut parts = command.split_whitespace();
        let verb = parts.next().unwrap_or("");
        let arg = parts.next().unwrap_or("");

        match verb {
            "" => String::new(),
            "status" => self.cmd_status(arg, now),
            "start" => self.cmd_start(arg, now),
            "stop" => self.cmd_stop(arg, now),
            "restart" => self.cmd_restart(arg, now),
            "pid" => self.cmd_pid(arg),
            "version" => format!("{}\n", env!("CARGO_PKG_VERSION")),
            "shutdown" => {
                SHUTDOWN_REQUESTED.store(true, Ordering::SeqCst);
                "Shutting down\n".to_string()
            }
            "help" => HELP_TEXT.to_string(),
            other => format!("*** Unknown command: {other}\n"),
        }
    }

    fn cmd_status(&self, arg: &str, now: Instant) -> String {
        let mut out = String::new();
        for p in &self.processes {
            if !arg.is_empty() && arg != "all" && p.name() != arg {
                continue;
            }
            out.push_str(&format!(
                "{:<28} {:<10} {}\n",
                p.name(),
                p.state.description(),
                p.status_description(now)
            ));
        }
        if out.is_empty() {
            if arg.is_empty() || arg == "all" {
                out.push_str("No programs configured\n");
            } else {
                out.push_str(&format!("{arg}: ERROR (no such process)\n"));
            }
        }
        out
    }

    fn cmd_start(&mut self, arg: &str, now: Instant) -> String {
        self.for_targets(arg, now, |p, now| {
            if p.start(now) {
                format!("{}: started\n", p.name())
            } else {
                format!("{}: ERROR (already started)\n", p.name())
            }
        })
    }

    fn cmd_stop(&mut self, arg: &str, now: Instant) -> String {
        self.for_targets(arg, now, |p, now| {
            if p.stop(now) {
                format!("{}: stopped\n", p.name())
            } else {
                format!("{}: ERROR (not running)\n", p.name())
            }
        })
    }

    fn cmd_restart(&mut self, arg: &str, now: Instant) -> String {
        // Initiate a stop; the process is automatically re-spawned by the
        // state machine once it has fully exited (see Process::request_restart).
        self.for_targets(arg, now, |p, now| {
            p.request_restart(now);
            format!("{}: restarted\n", p.name())
        })
    }

    fn cmd_pid(&self, arg: &str) -> String {
        if arg.is_empty() {
            return format!("{}\n", std::process::id());
        }
        for p in &self.processes {
            if p.name() == arg {
                return format!("{}\n", p.pid);
            }
        }
        format!("{arg}: ERROR (no such process)\n")
    }

    /// Apply `f` to every process matching `arg` (a name, or `all`/empty).
    fn for_targets<F>(&mut self, arg: &str, now: Instant, f: F) -> String
    where
        F: Fn(&mut Process, Instant) -> String,
    {
        let all = arg.is_empty() || arg == "all";
        let mut out = String::new();
        let mut matched = false;
        for p in &mut self.processes {
            if all || p.name() == arg {
                matched = true;
                let before = p.pid;
                let line = f(p, now);
                // Keep the pid index fresh if f spawned a process.
                let after = p.pid;
                let _ = (before, after); // index sync handled centrally below
                out.push_str(&line);
            }
        }
        // Rebuild pid index for any new pids created by f.
        self.resync_pid_index();
        if !matched {
            out.push_str(&format!("{arg}: ERROR (no such process)\n"));
        }
        out
    }

    /// Rebuild `pid_index` from the current process pids. Cheap (program
    /// counts are small) and keeps the map correct after control actions.
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
}

/// A short, syslog-ish timestamp for log lines.
fn now_timestamp() -> String {
    // Use libc localtime to avoid pulling in a date crate.
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

const HELP_TEXT: &str = "\
Available commands:
  status [name|all]   show process status
  start  <name|all>   start process(es)
  stop   <name|all>   stop process(es)
  restart <name|all>  restart process(es)
  pid [name]          show supervisord pid, or a process pid
  version             show supervisord version
  shutdown            stop supervisord and all its processes
  help                show this help
";
