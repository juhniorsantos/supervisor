//! The `supervisorctl` client entry point.
//!
//! Usage:
//!   supervisorctl [-c CONFIG] [COMMAND [ARGS...]]
//!
//! With a command, it runs once and prints the result. With no command, it
//! starts a small interactive shell.

use std::io::{self, BufRead, Write};
use std::path::PathBuf;
use std::process::exit;

use supervisor::config::Config;
use supervisor::control;

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
                // First non-option marks the start of the command.
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
        let command = rest.join(" ");
        run_one(&socket, &command);
    }
}

/// Determine the control socket path from the config file (if present) or
/// fall back to the conventional default.
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

fn run_one(socket: &std::path::Path, command: &str) {
    match control::request(socket, command) {
        Ok(resp) => {
            print!("{resp}");
            io::stdout().flush().ok();
        }
        Err(e) => {
            eprintln!(
                "supervisorctl: cannot reach supervisord at {}: {e}",
                socket.display()
            );
            exit(1);
        }
    }
}

fn repl(socket: &std::path::Path) {
    let stdin = io::stdin();
    let mut out = io::stdout();
    loop {
        print!("supervisor> ");
        out.flush().ok();
        let mut line = String::new();
        match stdin.lock().read_line(&mut line) {
            Ok(0) => {
                println!();
                break; // EOF
            }
            Ok(_) => {}
            Err(_) => break,
        }
        let command = line.trim();
        if command.is_empty() {
            continue;
        }
        if matches!(command, "quit" | "exit" | "bye") {
            break;
        }
        run_one(socket, command);
    }
}

fn print_usage() {
    println!(
        "supervisorctl {} — control client (Rust core)\n\n\
         Usage: supervisorctl [-c CONFIG] [COMMAND [ARGS...]]\n\n\
         Commands: status, start, stop, restart, pid, version, shutdown, help\n\
         With no command, an interactive shell is started.",
        env!("CARGO_PKG_VERSION")
    );
}
