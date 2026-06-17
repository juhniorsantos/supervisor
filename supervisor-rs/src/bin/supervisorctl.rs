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
        "reread" => cmd_reread(socket),
        "update" => cmd_update(socket, args),
        "add" => cmd_add(socket, args),
        "remove" => cmd_remove(socket, args),
        "signal" => cmd_signal(socket, args),
        "clear" => cmd_clear(socket, args),
        "fg" => cmd_fg(socket, args),
        "maintail" => match call(socket, "supervisor.readLog", &[Value::Int(0), Value::Int(0)]) {
            Ok(v) => print!("{}", as_str(&v)),
            Err(e) => fail_call(socket, e),
        },
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
        let namespec = make_namespec(&get_str(info, "group"), &name);
        if let Some(w) = want {
            if &name != w && &namespec != w {
                continue;
            }
        }
        shown = true;
        println!(
            "{:<28} {:<10} {}",
            namespec,
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

fn cmd_signal(socket: &Path, args: &[String]) {
    if args.len() < 2 {
        return println!("Error: signal requires a signal name and a process name");
    }
    let sig = &args[0];
    let names = &args[1..];
    if names.iter().any(|a| a == "all") {
        match call(socket, "supervisor.signalAllProcesses", &[Value::Str(sig.clone())]) {
            Ok(Value::Array(results)) => {
                for r in &results {
                    let name = get_str(r, "name");
                    let code = get_int(r, "status") as i32;
                    if code == 80 {
                        println!("{name}: signalled");
                    } else {
                        println!("{}", result_error(&name, code));
                    }
                }
            }
            Ok(_) => {}
            Err(e) => fail_call(socket, e),
        }
        return;
    }
    for name in names {
        match call(
            socket,
            "supervisor.signalProcess",
            &[Value::Str(name.clone()), Value::Str(sig.clone())],
        ) {
            Ok(_) => println!("{name}: signalled"),
            Err(ClientError::Fault(code, _)) => println!("{}", result_error(name, code)),
            Err(e) => return fail_call(socket, e),
        }
    }
}

fn cmd_clear(socket: &Path, args: &[String]) {
    if args.is_empty() {
        return println!("Error: clear requires a process name");
    }
    if args.iter().any(|a| a == "all") {
        match call(socket, "supervisor.clearAllProcessLogs", &[]) {
            Ok(Value::Array(results)) => {
                for r in &results {
                    println!("{}: cleared", get_str(r, "name"));
                }
            }
            Ok(_) => {}
            Err(e) => fail_call(socket, e),
        }
        return;
    }
    for name in args {
        match call(socket, "supervisor.clearProcessLogs", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: cleared"),
            Err(ClientError::Fault(code, _)) => println!("{}", result_error(name, code)),
            Err(e) => return fail_call(socket, e),
        }
    }
}

/// Foreground a running process: stream its stdout to the terminal and
/// forward terminal stdin to the process. Exit with Ctrl-D (EOF).
fn cmd_fg(socket: &Path, args: &[String]) {
    let Some(name) = args.first() else {
        return println!("ERROR: no process name supplied");
    };
    match call(socket, "supervisor.getProcessInfo", &[Value::Str(name.clone())]) {
        Ok(info) => {
            if get_int(&info, "state") != 20 {
                return println!("ERROR: process not running");
            }
        }
        Err(_) => return println!("ERROR: bad process name supplied"),
    }
    println!("==> Press Ctrl-D to exit <==");

    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::Arc;
    let stop = Arc::new(AtomicBool::new(false));

    // Background thread: follow the process stdout log.
    let tail_stop = stop.clone();
    let tail_socket = socket.to_path_buf();
    let tail_name = name.clone();
    let handle = std::thread::spawn(move || {
        let mut offset: i64 = 0;
        while !tail_stop.load(Ordering::Relaxed) {
            if let Ok(Value::Array(parts)) = control::call(
                &tail_socket,
                "supervisor.tailProcessStdoutLog",
                &[Value::Str(tail_name.clone()), Value::Int(offset), Value::Int(4096)],
            ) {
                if let (Some(data), Some(Value::Int(newoff))) = (parts.first(), parts.get(1)) {
                    let text = as_str(data);
                    if !text.is_empty() {
                        print!("{text}");
                        io::stdout().flush().ok();
                    }
                    offset = *newoff;
                }
            }
            std::thread::sleep(Duration::from_millis(200));
        }
    });

    // Foreground: forward our stdin to the process.
    let stdin = io::stdin();
    loop {
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => break, // Ctrl-D
            Ok(_) => {}
            Err(_) => break,
        }
        match call(socket, "supervisor.sendProcessStdin", &[Value::Str(name.clone()), Value::Str(line)]) {
            Ok(_) => {}
            Err(_) => {
                println!("Process got killed; exiting foreground");
                break;
            }
        }
    }
    stop.store(true, Ordering::Relaxed);
    let _ = handle.join();
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

fn cmd_reread(socket: &Path) {
    match reload_config(socket) {
        Ok((added, changed, removed)) => {
            let mut any = false;
            for (list, label) in [
                (&added, "available"),
                (&changed, "changed"),
                (&removed, "disappeared"),
            ] {
                for name in list {
                    println!("{name}: {label}");
                    any = true;
                }
            }
            if !any {
                println!("No config updates to processes");
            }
        }
        Err(e) => fail_call(socket, e),
    }
}

fn cmd_update(socket: &Path, args: &[String]) {
    let (added, changed, removed) = match reload_config(socket) {
        Ok(t) => t,
        Err(e) => return fail_call(socket, e),
    };
    let filter: Vec<&String> = args.iter().filter(|a| a.as_str() != "all").collect();
    let wanted = |g: &str| filter.is_empty() || filter.iter().any(|f| f.as_str() == g);

    for gname in &removed {
        if !wanted(gname) {
            continue;
        }
        let _ = call(socket, "supervisor.stopProcessGroup", &[Value::Str(gname.clone())]);
        println!("{gname}: stopped");
        match call(socket, "supervisor.removeProcessGroup", &[Value::Str(gname.clone())]) {
            Ok(_) => println!("{gname}: removed process group"),
            Err(ClientError::Fault(_, _)) => println!("{gname}: has problems; not removing"),
            Err(e) => return fail_call(socket, e),
        }
    }
    for gname in &changed {
        if !wanted(gname) {
            continue;
        }
        let _ = call(socket, "supervisor.stopProcessGroup", &[Value::Str(gname.clone())]);
        println!("{gname}: stopped");
        let _ = call(socket, "supervisor.removeProcessGroup", &[Value::Str(gname.clone())]);
        let _ = call(socket, "supervisor.addProcessGroup", &[Value::Str(gname.clone())]);
        println!("{gname}: updated process group");
    }
    for gname in &added {
        if !wanted(gname) {
            continue;
        }
        match call(socket, "supervisor.addProcessGroup", &[Value::Str(gname.clone())]) {
            Ok(_) => println!("{gname}: added process group"),
            Err(e) => return fail_call(socket, e),
        }
    }
}

fn cmd_add(socket: &Path, args: &[String]) {
    for name in args {
        match call(socket, "supervisor.addProcessGroup", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: added process group"),
            Err(ClientError::Fault(90, _)) => println!("ERROR: process group already active"),
            Err(ClientError::Fault(10, _)) => println!("ERROR: no such process/group: {name}"),
            Err(e) => return fail_call(socket, e),
        }
    }
}

fn cmd_remove(socket: &Path, args: &[String]) {
    for name in args {
        match call(socket, "supervisor.removeProcessGroup", &[Value::Str(name.clone())]) {
            Ok(_) => println!("{name}: removed process group"),
            Err(ClientError::Fault(91, _)) => {
                println!("ERROR: process/group still running: {name}")
            }
            Err(ClientError::Fault(10, _)) => println!("ERROR: no such process/group: {name}"),
            Err(e) => return fail_call(socket, e),
        }
    }
}

/// Group names `(added, changed, removed)` reported by a config reread.
type ConfigDiff = (Vec<String>, Vec<String>, Vec<String>);

/// Call `reloadConfig` and unpack the `[[added, changed, removed]]` result.
fn reload_config(socket: &Path) -> Result<ConfigDiff, ClientError> {
    let result = call(socket, "supervisor.reloadConfig", &[])?;
    let outer = match &result {
        Value::Array(items) => items.first(),
        _ => None,
    };
    let triple = match outer {
        Some(Value::Array(t)) if t.len() == 3 => t,
        _ => return Err(ClientError::Protocol("malformed reloadConfig result".into())),
    };
    Ok((
        str_list(&triple[0]),
        str_list(&triple[1]),
        str_list(&triple[2]),
    ))
}

fn str_list(v: &Value) -> Vec<String> {
    match v {
        Value::Array(items) => items.iter().map(as_str).collect(),
        _ => Vec::new(),
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
        11 => "bad signal",
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

/// `name` if the group equals the name, else `group:name` (matches the
/// original `make_namespec`).
fn make_namespec(group: &str, name: &str) -> String {
    if group == name || group.is_empty() {
        name.to_string()
    } else {
        format!("{group}:{name}")
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
         \x20 signal <SIG> <name|all> send a signal to process(es)\n\
         \x20 clear  <name|all>      clear process log(s)\n\
         \x20 tail   <name> [stderr] show a process log\n\
         \x20 fg     <name>          attach to a running process\n\
         \x20 maintail               show the main supervisord log\n\
         \x20 reread                 re-read config, report changes\n\
         \x20 update [group|all]     apply config changes (add/remove/restart groups)\n\
         \x20 add    <group>         activate a group from the config\n\
         \x20 remove <group>         deactivate a stopped group\n\
         \x20 pid    [name]          supervisord pid, or a process pid\n\
         \x20 version                supervisord version\n\
         \x20 reload                 restart supervisord\n\
         \x20 shutdown               stop supervisord\n\n\
         With no command, an interactive shell is started.",
        env!("CARGO_PKG_VERSION")
    );
}
