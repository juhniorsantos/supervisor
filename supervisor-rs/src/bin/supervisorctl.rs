//! The `supervisorctl` client entry point.
//!
//! Speaks XML-RPC over the unix control socket, the same protocol the daemon
//! exposes to the upstream Python `supervisorctl`.
//!
//! Usage:
//!   supervisorctl [-c CONFIG] [COMMAND [ARGS...]]

use std::io::{self, BufRead, Write};
use std::path::{Path, PathBuf};
use std::process::exit;
use std::time::{Duration, Instant};

use supervisor::config::Config;
use supervisor::control::{self, ClientError};
use supervisor::xmlrpc::Value;

const DEFAULT_CONFIG_PATHS: &[&str] = &[
    "supervisord.conf",
    "etc/supervisord.conf",
    "/etc/supervisord.conf",
    "/etc/supervisor/supervisord.conf",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<PathBuf> = None;
    let mut rest: Vec<String> = Vec::new();

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--configuration" => {
                i += 1;
                if i >= args.len() {
                    eprintln!("supervisorctl: missing argument to -c");
                    exit(2);
                }
                config_path = Some(PathBuf::from(&args[i]));
            }
            "-h" | "--help" => {
                print_usage();
                return;
            }
            _ => {
                rest.extend_from_slice(&args[i..]);
                break;
            }
        }
        i += 1;
    }

    let socket = resolve_socket(config_path);

    if rest.is_empty() {
        repl(&socket);
    } else {
        run_command(&socket, &rest);
    }
}

fn resolve_socket(config_path: Option<PathBuf>) -> PathBuf {
    let path = config_path.or_else(|| {
        DEFAULT_CONFIG_PATHS
            .iter()
            .map(PathBuf::from)
            .find(|p| p.exists())
    });
    if let Some(p) = path {
        if let Ok(cfg) = Config::load(&p) {
            if let Some(sock) = cfg.socket_path {
                return sock;
            }
        }
    }
    PathBuf::from("/tmp/supervisor.sock")
}

/// Execute one command line (a verb plus arguments).
fn run_command(socket: &Path, argv: &[String]) {
    let verb = argv[0].as_str();
    let args = &argv[1..];
    match verb {
        "status" => cmd_status(socket, args),
        "start" => cmd_start(socket, args),
        "stop" => cmd_stop(socket, args),
        "restart" => cmd_restart(socket, args),
        "pid" => cmd_pid(socket, args),
        "version" => match call(socket, "supervisor.getSupervisorVersion", &[]) {
            Ok(v) => println!("{}", as_str(&v)),
            Err(e) => fail_call(socket, e),
        },
        "avail" | "tail" => cmd_tail(socket, args),
        "shutdown" => match call(socket, "supervisor.shutdown", &[]) {
            Ok(_) => println!("Shut down"),
            Err(e) => fail_call(socket, e),
        },
        "reload" => match call(socket, "supervisor.restart", &[]) {
            Ok(_) => println!("Restarted supervisord"),
            Err(e) => fail_call(socket, e),
        },
        "help" => print_usage(),
        other => println!("*** Unknown syntax: {other}"),
    }
}

fn cmd_status(socket: &Path, args: &[String]) {
    let infos = match call(socket, "supervisor.getAllProcessInfo", &[]) {
        Ok(Value::Array(items)) => items,
        Ok(_) => Vec::new(),
        Err(e) => return fail_call(socket, e),
    };
    let want: Option<&String> = args.first().filter(|a| a.as_str() != "all");
    let mut shown = false;
    for info in &infos {
        let name = get_str(info, "name");
        if let Some(w) = want {
            if &name != w {
                continue;
            }
        }
        shown = true;
        println!(
            "{:<28} {:<10} {}",
            name,
            get_str(info, "statename"),
            get_str(info, "description")
        );
    }
    if !shown {
        if let Some(w) = want {
            println!("{w}: ERROR (no such process)");
        }
    }
}

fn cmd_start(socket: &Path, args: &[String]) {
    if args.is_empty() {
        return println!("Error: start requires a process name");
    }
    if args.iter().any(|a| a == "all") {
        match call(socket, "supervisor.startAllProcesses", &[]) {
            Ok(Value::Array(results)) => print_results(&results),
            Ok(_) => {}
            Err(e) => fail_call(socket, e),
        }
        return;
    }
    for name in args {
        match call(socket, "supervisor.startProcess", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: started"),
            Err(ClientError::Fault(code, _)) => println!("{}", result_error(name, code)),
            Err(e) => return fail_call(socket, e),
        }
    }
}

fn cmd_stop(socket: &Path, args: &[String]) {
    if args.is_empty() {
        return println!("Error: stop requires a process name");
    }
    if args.iter().any(|a| a == "all") {
        match call(socket, "supervisor.stopAllProcesses", &[]) {
            Ok(Value::Array(results)) => print_results(&results),
            Ok(_) => {}
            Err(e) => fail_call(socket, e),
        }
        return;
    }
    for name in args {
        match call(socket, "supervisor.stopProcess", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: stopped"),
            Err(ClientError::Fault(code, _)) => println!("{}", result_error(name, code)),
            Err(e) => return fail_call(socket, e),
        }
    }
}

/// Restart mirrors the upstream client: stop, wait for the process to settle,
/// then start.
fn cmd_restart(socket: &Path, args: &[String]) {
    if args.is_empty() {
        return println!("Error: restart requires a process name");
    }
    let all = args.iter().any(|a| a == "all");
    let names: Vec<String> = if all {
        match call(socket, "supervisor.getAllProcessInfo", &[]) {
            Ok(Value::Array(items)) => items.iter().map(|i| get_str(i, "name")).collect(),
            _ => Vec::new(),
        }
    } else {
        args.to_vec()
    };

    for name in &names {
        let _ = call(socket, "supervisor.stopProcess", &[Value::Str(name.clone())]);
        wait_until_stopped(socket, name, Duration::from_secs(12));
        println!("{name}: stopped");
    }
    for name in &names {
        match call(socket, "supervisor.startProcess", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: started"),
            Err(ClientError::Fault(code, _)) => println!("{}", result_error(name, code)),
            Err(e) => return fail_call(socket, e),
        }
    }
}

fn cmd_pid(socket: &Path, args: &[String]) {
    match args.first() {
        None => match call(socket, "supervisor.getPID", &[]) {
            Ok(v) => println!("{}", as_int(&v)),
            Err(e) => fail_call(socket, e),
        },
        Some(name) => match call(socket, "supervisor.getProcessInfo", &[Value::Str(name.clone())]) {
            Ok(info) => println!("{}", get_int(&info, "pid")),
            Err(ClientError::Fault(_, _)) => println!("{name}: ERROR (no such process)"),
            Err(e) => fail_call(socket, e),
        },
    }
}

fn cmd_tail(socket: &Path, args: &[String]) {
    let Some(name) = args.first() else {
        return println!("Error: tail requires a process name");
    };
    let channel = if args.iter().any(|a| a == "stderr") {
        "supervisor.readProcessStderrLog"
    } else {
        "supervisor.readProcessStdoutLog"
    };
    match call(socket, channel, &[Value::Str(name.clone()), Value::Int(0), Value::Int(0)]) {
        Ok(v) => print!("{}", as_str(&v)),
        Err(ClientError::Fault(_, _)) => println!("{name}: ERROR (no log file)"),
        Err(e) => fail_call(socket, e),
    }
}

/// Poll `getProcessInfo` until the process reaches a stopped state or the
/// timeout elapses.
fn wait_until_stopped(socket: &Path, name: &str, timeout: Duration) {
    let start = Instant::now();
    while start.elapsed() < timeout {
        match call(socket, "supervisor.getProcessInfo", &[Value::Str(name.to_string())]) {
            Ok(info) => {
                let state = get_int(&info, "state");
                // STOPPED=0, EXITED=100, FATAL=200, UNKNOWN=1000
                if matches!(state, 0 | 100 | 200 | 1000) {
                    return;
                }
            }
            Err(_) => return,
        }
        std::thread::sleep(Duration::from_millis(150));
    }
}

fn print_results(results: &[Value]) {
    for r in results {
        let name = get_str(r, "name");
        let code = get_int(r, "status") as i32;
        if code == 80 {
            println!("{name}: done");
        } else {
            println!("{}", result_error(&name, code));
        }
    }
}

/// Map a fault code to the upstream-style error string.
fn result_error(name: &str, code: i32) -> String {
    let reason = match code {
        10 => "no such process",
        20 => "no such file",
        21 => "file is not executable",
        50 => "spawn error",
        60 => "already started",
        70 => "not running",
        _ => "error",
    };
    format!("{name}: ERROR ({reason})")
}

// -- XML-RPC value helpers --------------------------------------------------

fn call(socket: &Path, method: &str, params: &[Value]) -> Result<Value, ClientError> {
    control::call(socket, method, params)
}

fn as_str(v: &Value) -> String {
    match v {
        Value::Str(s) => s.clone(),
        Value::Int(i) => i.to_string(),
        Value::Bool(b) => b.to_string(),
        _ => String::new(),
    }
}

fn as_int(v: &Value) -> i64 {
    match v {
        Value::Int(i) => *i,
        _ => 0,
    }
}

fn get_str(v: &Value, key: &str) -> String {
    if let Value::Struct(members) = v {
        for (k, val) in members {
            if k == key {
                return as_str(val);
            }
        }
    }
    String::new()
}

fn get_int(v: &Value, key: &str) -> i64 {
    if let Value::Struct(members) = v {
        for (k, val) in members {
            if k == key {
                return as_int(val);
            }
        }
    }
    0
}

fn fail_call(socket: &Path, e: ClientError) {
    match e {
        ClientError::Io(_) => {
            eprintln!(
                "supervisorctl: cannot reach supervisord at {} (is it running?)",
                socket.display()
            );
            exit(1);
        }
        other => {
            eprintln!("supervisorctl: {other}");
            exit(1);
        }
    }
}

fn repl(socket: &Path) {
    let stdin = io::stdin();
    let mut out = io::stdout();
    loop {
        print!("supervisor> ");
        out.flush().ok();
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                println!();
                break;
            }
            Ok(_) => {}
            Err(_) => break,
        }
        let parts: Vec<String> = line.split_whitespace().map(|s| s.to_string()).collect();
        if parts.is_empty() {
            continue;
        }
        if matches!(parts[0].as_str(), "quit" | "exit" | "bye") {
            break;
        }
        run_command(socket, &parts);
    }
}

fn print_usage() {
    println!(
        "supervisorctl {} — control client (Rust core)\n\n\
         Usage: supervisorctl [-c CONFIG] [COMMAND [ARGS...]]\n\n\
         Commands:\n\
         \x20 status [name|all]      show process status\n\
         \x20 start  <name|all>      start process(es)\n\
         \x20 stop   <name|all>      stop process(es)\n\
         \x20 restart <name|all>     restart process(es)\n\
         \x20 tail   <name> [stderr] show a process log\n\
         \x20 pid    [name]          supervisord pid, or a process pid\n\
         \x20 version                supervisord version\n\
         \x20 reload                 restart supervisord\n\
         \x20 shutdown               stop supervisord\n\n\
         With no command, an interactive shell is started.",
        env!("CARGO_PKG_VERSION")
    );
}
