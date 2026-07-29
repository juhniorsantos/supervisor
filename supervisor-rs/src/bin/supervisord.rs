//! The `supervisord` daemon entry point.
//!
//! Usage:
//!   supervisord [-c /path/to/supervisord.conf] [-n]
//!
//!   -c FILE   configuration file (default: search standard locations)
//!   -n        run in the foreground (do not daemonize)
//!   -h        show this help

use std::io::Write;
use std::path::{Path, PathBuf};
use std::process::exit;

use supervisor::config::Config;
use supervisor::daemon::Supervisor;

const DEFAULT_CONFIG_PATHS: &[&str] = &[
    "supervisord.conf",
    "etc/supervisord.conf",
    "/etc/supervisord.conf",
    "/etc/supervisor/supervisord.conf",
];

fn main() {
    let args: Vec<String> = std::env::args().collect();
    let mut config_path: Option<PathBuf> = None;
    let mut nodaemon = false;

    let mut i = 1;
    while i < args.len() {
        match args[i].as_str() {
            "-c" | "--configuration" => {
                i += 1;
                if i >= args.len() {
                    fail("missing argument to -c");
                }
                config_path = Some(PathBuf::from(&args[i]));
            }
            "-n" | "--nodaemon" => nodaemon = true,
            "-h" | "--help" => {
                print_usage();
                return;
            }
            other => fail(&format!("unknown argument: {other}")),
        }
        i += 1;
    }

    let config_path = match config_path.or_else(find_default_config) {
        Some(p) => p,
        None => fail("no config file found (use -c) and no default exists"),
    };

    let mut config = match Config::load(&config_path) {
        Ok(c) => c,
        Err(e) => fail(&format!("config error: {e}")),
    };

    // The -n flag overrides the config file.
    if nodaemon {
        config.supervisord.nodaemon = true;
    }

    if let Some(dir) = &config.supervisord.directory {
        let _ = std::env::set_current_dir(dir);
    }

    if !config.supervisord.nodaemon {
        daemonize();
    }

    // Write the pidfile (now that we are the final process).
    let pidfile = config.supervisord.pidfile.clone();
    if let Err(e) = write_pidfile(&pidfile) {
        eprintln!("warning: could not write pidfile {}: {e}", pidfile.display());
    }

    let mut supervisor = match Supervisor::new(config) {
        Ok(s) => s,
        Err(e) => fail(&format!("startup error: {e}")),
    };
    supervisor.run();
}

fn find_default_config() -> Option<PathBuf> {
    DEFAULT_CONFIG_PATHS
        .iter()
        .map(PathBuf::from)
        .find(|p| p.exists())
}

fn write_pidfile(path: &Path) -> std::io::Result<()> {
    let mut f = std::fs::File::create(path)?;
    writeln!(f, "{}", std::process::id())
}

/// Standard double-fork daemonization: detach from the controlling terminal
/// and redirect the standard streams to /dev/null.
fn daemonize() {
    unsafe {
        // First fork: parent exits, child continues.
        match libc::fork() {
            -1 => fail("fork failed"),
            0 => {}             // child
            _ => exit(0),       // parent
        }

        // Become a session leader to lose the controlling terminal.
        if libc::setsid() == -1 {
            fail("setsid failed");
        }

        // Second fork: ensure we can never reacquire a controlling terminal.
        match libc::fork() {
            -1 => fail("fork failed"),
            0 => {}
            _ => exit(0),
        }

        // Redirect stdin/stdout/stderr to /dev/null.
        let devnull = libc::open(c"/dev/null".as_ptr(), libc::O_RDWR);
        if devnull >= 0 {
            libc::dup2(devnull, 0);
            libc::dup2(devnull, 1);
            libc::dup2(devnull, 2);
            if devnull > 2 {
                libc::close(devnull);
            }
        }
    }
}

fn print_usage() {
    println!(
        "supervisord {} — process control system (Rust core)\n\n\
         Usage: supervisord [-c CONFIG] [-n]\n\n\
         Options:\n\
         \x20 -c, --configuration FILE   path to supervisord.conf\n\
         \x20 -n, --nodaemon             run in the foreground\n\
         \x20 -h, --help                 show this help",
        env!("CARGO_PKG_VERSION")
    );
}

fn fail(msg: &str) -> ! {
    eprintln!("supervisord: {msg}");
    exit(2);
}
