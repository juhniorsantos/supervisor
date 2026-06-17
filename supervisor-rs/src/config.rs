//! Parsing of `supervisord.conf` (a useful subset).
//!
//! The format is the classic INI used by the original Supervisor:
//!
//! ```ini
//! [unix_http_server]
//! file=/tmp/supervisor.sock
//!
//! [supervisord]
//! logfile=/tmp/supervisord.log
//!
//! [program:web]
//! command=/usr/bin/python -m http.server 8000
//! autostart=true
//! autorestart=unexpected
//! ```
//!
//! Comments start with `;` or `#`. Inline comments must be preceded by
//! whitespace (e.g. `key=value   ; comment`), matching the original.

use std::collections::HashMap;
use std::path::PathBuf;

/// When and whether a program should be automatically restarted after it
/// exits on its own.
#[derive(Clone, Copy, PartialEq, Eq, Debug)]
pub enum AutoRestart {
    /// Never restart automatically.
    Never,
    /// Always restart, regardless of exit code.
    Always,
    /// Restart only if the exit code is not in `exitcodes`.
    Unexpected,
}

/// Where a process's stdout/stderr stream should be written.
#[derive(Clone, Debug)]
pub enum LogTarget {
    /// Resolve to `<childlogdir>/<name>-<stream>.log` automatically.
    Auto,
    /// Discard the stream.
    None,
    /// Write to an explicit path.
    Path(PathBuf),
}

/// Configuration for a single supervised program instance.
#[derive(Clone, Debug)]
pub struct ProgramConfig {
    pub name: String,
    pub command: String,
    pub directory: Option<PathBuf>,
    pub autostart: bool,
    pub autorestart: AutoRestart,
    pub startsecs: u64,
    pub startretries: u32,
    pub exitcodes: Vec<i32>,
    pub stopsignal: i32,
    pub stopwaitsecs: u64,
    pub environment: Vec<(String, String)>,
    pub user: Option<String>,
    pub umask: Option<u32>,
    pub priority: i32,
    pub redirect_stderr: bool,
    pub stdout_logfile: LogTarget,
    pub stdout_logfile_maxbytes: u64,
    pub stdout_logfile_backups: u32,
    pub stderr_logfile: LogTarget,
    pub stderr_logfile_maxbytes: u64,
    pub stderr_logfile_backups: u32,
}

/// Configuration for the `[supervisord]` section.
#[derive(Clone, Debug)]
pub struct SupervisordConfig {
    pub logfile: PathBuf,
    pub logfile_maxbytes: u64,
    pub logfile_backups: u32,
    pub loglevel: String,
    pub pidfile: PathBuf,
    pub nodaemon: bool,
    pub silent: bool,
    pub childlogdir: PathBuf,
    pub directory: Option<PathBuf>,
    pub identifier: String,
    pub umask: Option<u32>,
    pub environment: Vec<(String, String)>,
}

impl Default for SupervisordConfig {
    fn default() -> Self {
        SupervisordConfig {
            logfile: PathBuf::from("supervisord.log"),
            logfile_maxbytes: 50 * 1024 * 1024,
            logfile_backups: 10,
            loglevel: "info".to_string(),
            pidfile: PathBuf::from("supervisord.pid"),
            nodaemon: false,
            silent: false,
            childlogdir: default_tmpdir(),
            directory: None,
            identifier: "supervisor".to_string(),
            umask: None,
            environment: Vec::new(),
        }
    }
}

/// The fully parsed configuration file.
#[derive(Clone, Debug)]
pub struct Config {
    pub supervisord: SupervisordConfig,
    pub programs: Vec<ProgramConfig>,
    /// Path to the unix control socket (`[unix_http_server] file=`).
    pub socket_path: Option<PathBuf>,
}

fn default_tmpdir() -> PathBuf {
    std::env::var_os("TMPDIR")
        .map(PathBuf::from)
        .unwrap_or_else(|| PathBuf::from("/tmp"))
}

/// A parsed `[section]` with its key/value pairs, in file order.
struct Section {
    name: String,
    items: Vec<(String, String)>,
}

/// Tokenise the INI text into ordered sections. Returns an error string on
/// malformed input.
fn parse_ini(text: &str) -> Result<Vec<Section>, String> {
    let mut sections: Vec<Section> = Vec::new();
    let mut current: Option<Section> = None;
    let mut pending_key: Option<usize> = None; // index into current.items for line continuations

    for (lineno, raw) in text.lines().enumerate() {
        let lineno = lineno + 1;

        // Full-line comments / blank lines.
        let trimmed = raw.trim();
        if trimmed.is_empty() || trimmed.starts_with(';') || trimmed.starts_with('#') {
            continue;
        }

        // Section header.
        if trimmed.starts_with('[') {
            if let Some(sec) = current.take() {
                sections.push(sec);
            }
            let end = trimmed
                .find(']')
                .ok_or_else(|| format!("line {lineno}: missing ']' in section header"))?;
            let name = trimmed[1..end].trim().to_string();
            current = Some(Section {
                name,
                items: Vec::new(),
            });
            pending_key = None;
            continue;
        }

        // Indented continuation of the previous value.
        if (raw.starts_with(' ') || raw.starts_with('\t')) && pending_key.is_some() {
            if let (Some(sec), Some(idx)) = (current.as_mut(), pending_key) {
                sec.items[idx].1.push('\n');
                sec.items[idx].1.push_str(trimmed);
                continue;
            }
        }

        // key=value or key:value
        let sep = trimmed.find('=').or_else(|| trimmed.find(':'));
        let sep = sep.ok_or_else(|| format!("line {lineno}: expected 'key=value'"))?;
        let key = trimmed[..sep].trim().to_string();
        let value = strip_inline_comment(&trimmed[sep + 1..]);

        let sec = current
            .as_mut()
            .ok_or_else(|| format!("line {lineno}: key '{key}' outside any [section]"))?;
        sec.items.push((key, value));
        pending_key = Some(sec.items.len() - 1);
    }

    if let Some(sec) = current.take() {
        sections.push(sec);
    }
    Ok(sections)
}

/// Strip an inline `;` comment. Per the original, the `;` must be preceded
/// by whitespace to count as a comment.
fn strip_inline_comment(value: &str) -> String {
    let bytes = value.as_bytes();
    let mut i = 0;
    while i < bytes.len() {
        if bytes[i] == b';' && i > 0 && (bytes[i - 1] == b' ' || bytes[i - 1] == b'\t') {
            return value[..i].trim().to_string();
        }
        i += 1;
    }
    value.trim().to_string()
}

fn items_map(items: &[(String, String)]) -> HashMap<&str, &str> {
    let mut m = HashMap::new();
    for (k, v) in items {
        m.insert(k.as_str(), v.as_str());
    }
    m
}

/// Parse a human byte size like `50MB`, `1KB`, `10` into bytes. Uses
/// 1024-based units, matching the original `datatypes.byte_size`.
pub fn parse_byte_size(s: &str) -> Result<u64, String> {
    let s = s.trim();
    let lower = s.to_ascii_lowercase();
    let (num, mult): (&str, u64) = if let Some(n) = lower.strip_suffix("gb") {
        (n, 1024 * 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("mb") {
        (n, 1024 * 1024)
    } else if let Some(n) = lower.strip_suffix("kb") {
        (n, 1024)
    } else if let Some(n) = lower.strip_suffix("b") {
        (n, 1)
    } else {
        (lower.as_str(), 1)
    };
    let num: u64 = num
        .trim()
        .parse()
        .map_err(|_| format!("invalid byte size: {s:?}"))?;
    Ok(num * mult)
}

/// Parse a boolean as the original does (true/false/yes/no/on/off/1/0).
pub fn parse_bool(s: &str) -> Result<bool, String> {
    match s.trim().to_ascii_lowercase().as_str() {
        "true" | "yes" | "on" | "1" => Ok(true),
        "false" | "no" | "off" | "0" => Ok(false),
        other => Err(format!("invalid boolean: {other:?}")),
    }
}

/// Map a signal name (with or without the `SIG` prefix) to its number.
pub fn parse_signal(s: &str) -> Result<i32, String> {
    let name = s.trim().to_ascii_uppercase();
    let name = name.strip_prefix("SIG").unwrap_or(&name);
    let sig = match name {
        "TERM" => libc::SIGTERM,
        "KILL" => libc::SIGKILL,
        "INT" => libc::SIGINT,
        "QUIT" => libc::SIGQUIT,
        "HUP" => libc::SIGHUP,
        "USR1" => libc::SIGUSR1,
        "USR2" => libc::SIGUSR2,
        "STOP" => libc::SIGSTOP,
        "CONT" => libc::SIGCONT,
        _ => return Err(format!("unknown signal: {s:?}")),
    };
    Ok(sig)
}

/// Parse an `environment=` value: `A="1",B="2"` into pairs. Quotes around
/// values are optional and stripped.
fn parse_environment(s: &str) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for pair in split_top_level_commas(s) {
        let pair = pair.trim();
        if pair.is_empty() {
            continue;
        }
        if let Some(eq) = pair.find('=') {
            let k = pair[..eq].trim().to_string();
            let mut v = pair[eq + 1..].trim().to_string();
            if v.len() >= 2 && v.starts_with('"') && v.ends_with('"') {
                v = v[1..v.len() - 1].to_string();
            }
            out.push((k, v));
        }
    }
    out
}

/// Split on commas that are not inside double quotes.
fn split_top_level_commas(s: &str) -> Vec<String> {
    let mut out = Vec::new();
    let mut buf = String::new();
    let mut in_quotes = false;
    for c in s.chars() {
        match c {
            '"' => {
                in_quotes = !in_quotes;
                buf.push(c);
            }
            ',' if !in_quotes => {
                out.push(std::mem::take(&mut buf));
            }
            _ => buf.push(c),
        }
    }
    if !buf.is_empty() {
        out.push(buf);
    }
    out
}

/// Expand `%(program_name)s`, `%(process_num)d` (and zero-padded variants
/// like `%(process_num)02d`) in a process_name template.
fn expand_process_name(template: &str, program_name: &str, process_num: usize) -> String {
    let mut out = template.replace("%(program_name)s", program_name);
    // Handle %(process_num)0Nd and %(process_num)d.
    while let Some(start) = out.find("%(process_num)") {
        let rest = &out[start + "%(process_num)".len()..];
        // Collect the format spec up to and including 'd'.
        let mut padded_width = 0usize;
        let bytes = rest.as_bytes();
        // optional leading zeros / width digits
        let mut j = 0;
        while j < bytes.len() && bytes[j].is_ascii_digit() {
            padded_width = padded_width * 10 + (bytes[j] - b'0') as usize;
            j += 1;
        }
        if j >= bytes.len() || bytes[j] != b'd' {
            // Not a recognised spec; bail to avoid an infinite loop.
            break;
        }
        let spec_len = j + 1;
        let replacement = if padded_width > 0 {
            format!("{:0width$}", process_num, width = padded_width)
        } else {
            process_num.to_string()
        };
        let end = start + "%(process_num)".len() + spec_len;
        out.replace_range(start..end, &replacement);
    }
    out
}

impl Config {
    /// Load and parse a configuration file from `path`.
    pub fn load(path: &std::path::Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read config {}: {e}", path.display()))?;
        Config::parse(&text)
    }

    /// Parse configuration from a string.
    pub fn parse(text: &str) -> Result<Config, String> {
        let sections = parse_ini(text)?;
        let mut supervisord = SupervisordConfig::default();
        let mut socket_path = None;
        let mut programs = Vec::new();

        for sec in &sections {
            if sec.name == "supervisord" {
                supervisord = parse_supervisord(&sec.items)?;
            } else if sec.name == "unix_http_server" {
                let m = items_map(&sec.items);
                if let Some(f) = m.get("file") {
                    socket_path = Some(PathBuf::from(*f));
                }
            } else if let Some(prog) = sec.name.strip_prefix("program:") {
                let expanded = parse_program(prog.trim(), &sec.items, &supervisord)?;
                programs.extend(expanded);
            }
            // Other sections (rpcinterface, supervisorctl, eventlistener,
            // inet_http_server, group) are accepted but ignored in this core.
        }

        // Order programs by priority for deterministic startup.
        programs.sort_by_key(|p| p.priority);

        Ok(Config {
            supervisord,
            programs,
            socket_path,
        })
    }
}

fn parse_supervisord(items: &[(String, String)]) -> Result<SupervisordConfig, String> {
    let m = items_map(items);
    let mut c = SupervisordConfig::default();
    if let Some(v) = m.get("logfile") {
        c.logfile = PathBuf::from(*v);
    }
    if let Some(v) = m.get("logfile_maxbytes") {
        c.logfile_maxbytes = parse_byte_size(v)?;
    }
    if let Some(v) = m.get("logfile_backups") {
        c.logfile_backups = v.parse().map_err(|_| "invalid logfile_backups")?;
    }
    if let Some(v) = m.get("loglevel") {
        c.loglevel = v.to_string();
    }
    if let Some(v) = m.get("pidfile") {
        c.pidfile = PathBuf::from(*v);
    }
    if let Some(v) = m.get("nodaemon") {
        c.nodaemon = parse_bool(v)?;
    }
    if let Some(v) = m.get("silent") {
        c.silent = parse_bool(v)?;
    }
    if let Some(v) = m.get("childlogdir") {
        c.childlogdir = PathBuf::from(*v);
    }
    if let Some(v) = m.get("directory") {
        c.directory = Some(PathBuf::from(*v));
    }
    if let Some(v) = m.get("identifier") {
        c.identifier = v.to_string();
    }
    if let Some(v) = m.get("umask") {
        c.umask = Some(u32::from_str_radix(v.trim_start_matches("0o").trim(), 8)
            .map_err(|_| "invalid umask")?);
    }
    if let Some(v) = m.get("environment") {
        c.environment = parse_environment(v);
    }
    Ok(c)
}

fn parse_program(
    name: &str,
    items: &[(String, String)],
    supervisord: &SupervisordConfig,
) -> Result<Vec<ProgramConfig>, String> {
    let m = items_map(items);

    let command = m
        .get("command")
        .ok_or_else(|| format!("[program:{name}] is missing required 'command'"))?
        .to_string();

    let numprocs: usize = m
        .get("numprocs")
        .map(|v| v.parse())
        .transpose()
        .map_err(|_| "invalid numprocs")?
        .unwrap_or(1);

    let process_name_tmpl = m
        .get("process_name")
        .map(|s| s.to_string())
        .unwrap_or_else(|| "%(program_name)s".to_string());

    let directory = m.get("directory").map(PathBuf::from);
    let autostart = m.get("autostart").map(|v| parse_bool(v)).transpose()?.unwrap_or(true);
    let autorestart = match m.get("autorestart").map(|s| s.trim().to_ascii_lowercase()) {
        None => AutoRestart::Unexpected,
        Some(s) => match s.as_str() {
            "true" | "yes" | "on" | "1" => AutoRestart::Always,
            "false" | "no" | "off" | "0" => AutoRestart::Never,
            "unexpected" => AutoRestart::Unexpected,
            other => return Err(format!("invalid autorestart: {other:?}")),
        },
    };
    let startsecs = m.get("startsecs").map(|v| v.parse()).transpose().map_err(|_| "invalid startsecs")?.unwrap_or(1);
    let startretries = m.get("startretries").map(|v| v.parse()).transpose().map_err(|_| "invalid startretries")?.unwrap_or(3);
    let exitcodes = match m.get("exitcodes") {
        None => vec![0],
        Some(v) => {
            let mut codes = Vec::new();
            for part in v.split(',') {
                let part = part.trim();
                if part.is_empty() {
                    continue;
                }
                codes.push(part.parse().map_err(|_| format!("invalid exitcode: {part:?}"))?);
            }
            if codes.is_empty() {
                vec![0]
            } else {
                codes
            }
        }
    };
    let stopsignal = m.get("stopsignal").map(|v| parse_signal(v)).transpose()?.unwrap_or(libc::SIGTERM);
    let stopwaitsecs = m.get("stopwaitsecs").map(|v| v.parse()).transpose().map_err(|_| "invalid stopwaitsecs")?.unwrap_or(10);
    let user = m.get("user").map(|s| s.to_string());
    let umask = match m.get("umask") {
        None => None,
        Some(v) => Some(u32::from_str_radix(v.trim_start_matches("0o").trim(), 8).map_err(|_| "invalid umask")?),
    };
    let priority = m.get("priority").map(|v| v.parse()).transpose().map_err(|_| "invalid priority")?.unwrap_or(999);
    let redirect_stderr = m.get("redirect_stderr").map(|v| parse_bool(v)).transpose()?.unwrap_or(false);

    let stdout_logfile = parse_log_target(m.get("stdout_logfile").copied());
    let stdout_logfile_maxbytes = m.get("stdout_logfile_maxbytes").map(|v| parse_byte_size(v)).transpose()?.unwrap_or(50 * 1024 * 1024);
    let stdout_logfile_backups = m.get("stdout_logfile_backups").map(|v| v.parse()).transpose().map_err(|_| "invalid stdout_logfile_backups")?.unwrap_or(10);
    let stderr_logfile = parse_log_target(m.get("stderr_logfile").copied());
    let stderr_logfile_maxbytes = m.get("stderr_logfile_maxbytes").map(|v| parse_byte_size(v)).transpose()?.unwrap_or(50 * 1024 * 1024);
    let stderr_logfile_backups = m.get("stderr_logfile_backups").map(|v| v.parse()).transpose().map_err(|_| "invalid stderr_logfile_backups")?.unwrap_or(10);

    let mut environment = supervisord.environment.clone();
    if let Some(v) = m.get("environment") {
        environment.extend(parse_environment(v));
    }

    let mut out = Vec::with_capacity(numprocs);
    for i in 0..numprocs.max(1) {
        let mut inst_name = expand_process_name(&process_name_tmpl, name, i);
        // Guard against duplicate names when numprocs > 1 but the template
        // doesn't vary per instance.
        if numprocs > 1 && inst_name == name {
            inst_name = format!("{name}_{i:02}");
        }
        out.push(ProgramConfig {
            name: inst_name,
            command: command.clone(),
            directory: directory.clone(),
            autostart,
            autorestart,
            startsecs,
            startretries,
            exitcodes: exitcodes.clone(),
            stopsignal,
            stopwaitsecs,
            environment: environment.clone(),
            user: user.clone(),
            umask,
            priority,
            redirect_stderr,
            stdout_logfile: stdout_logfile.clone(),
            stdout_logfile_maxbytes,
            stdout_logfile_backups,
            stderr_logfile: stderr_logfile.clone(),
            stderr_logfile_maxbytes,
            stderr_logfile_backups,
        });
    }
    Ok(out)
}

fn parse_log_target(value: Option<&str>) -> LogTarget {
    match value {
        None | Some("AUTO") => LogTarget::Auto,
        Some("NONE") => LogTarget::None,
        Some(p) => LogTarget::Path(PathBuf::from(p)),
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parses_byte_sizes() {
        assert_eq!(parse_byte_size("50MB").unwrap(), 50 * 1024 * 1024);
        assert_eq!(parse_byte_size("1KB").unwrap(), 1024);
        assert_eq!(parse_byte_size("512").unwrap(), 512);
        assert_eq!(parse_byte_size("1GB").unwrap(), 1024 * 1024 * 1024);
    }

    #[test]
    fn strips_inline_comments_only_with_leading_space() {
        assert_eq!(strip_inline_comment("value ; comment"), "value");
        assert_eq!(strip_inline_comment("a=b;c"), "a=b;c");
    }

    #[test]
    fn expands_process_names() {
        assert_eq!(expand_process_name("%(program_name)s", "web", 0), "web");
        assert_eq!(
            expand_process_name("%(program_name)s_%(process_num)02d", "web", 3),
            "web_03"
        );
    }

    #[test]
    fn parses_a_basic_program() {
        let text = "\
[supervisord]
logfile=/tmp/sd.log

[unix_http_server]
file=/tmp/sup.sock

[program:web]
command=/bin/sleep 100
autostart=true
autorestart=unexpected
numprocs=2
process_name=%(program_name)s_%(process_num)02d
";
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.socket_path, Some(PathBuf::from("/tmp/sup.sock")));
        assert_eq!(cfg.programs.len(), 2);
        assert_eq!(cfg.programs[0].name, "web_00");
        assert_eq!(cfg.programs[1].name, "web_01");
        assert_eq!(cfg.programs[0].autorestart, AutoRestart::Unexpected);
    }

    #[test]
    fn parses_environment_pairs() {
        let env = parse_environment(r#"A="1",B="2,3""#);
        assert_eq!(env, vec![("A".into(), "1".into()), ("B".into(), "2,3".into())]);
    }
}
