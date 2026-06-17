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
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum LogTarget {
    /// Resolve to `<childlogdir>/<name>-<stream>.log` automatically.
    Auto,
    /// Discard the stream.
    None,
    /// Write to an explicit path.
    Path(PathBuf),
}

/// Configuration for a single supervised program instance.
#[derive(Clone, Debug, PartialEq)]
pub struct ProgramConfig {
    pub name: String,
    /// The base program/eventlistener section name (before numprocs expansion).
    pub program: String,
    /// The group this instance belongs to (defaults to `program`).
    pub group: String,
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
    /// True for `[eventlistener:x]` sections: the process speaks the event
    /// notification protocol on its stdin/stdout.
    pub is_listener: bool,
    /// Event type names this listener subscribes to (abstract or concrete).
    pub events: Vec<String>,
    /// Listener event buffer size.
    pub buffer_size: usize,
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

/// Optional HTTP Basic auth credentials for a control server.
#[derive(Clone, Debug, Default)]
pub struct HttpAuth {
    pub username: Option<String>,
    pub password: Option<String>,
}

impl HttpAuth {
    pub fn is_set(&self) -> bool {
        self.username.is_some()
    }
}

/// The fully parsed configuration file.
#[derive(Clone, Debug)]
pub struct Config {
    pub supervisord: SupervisordConfig,
    pub programs: Vec<ProgramConfig>,
    /// Path to the unix control socket (`[unix_http_server] file=`).
    pub socket_path: Option<PathBuf>,
    /// Basic-auth credentials for the unix socket, if configured.
    pub unix_auth: HttpAuth,
    /// `ip:port` from `[inet_http_server] port=`, if the section is present.
    pub inet_addr: Option<String>,
    /// Basic-auth credentials for the inet server, if configured.
    pub inet_auth: HttpAuth,
    /// The path this config was loaded from (for `reread`/`reloadConfig`).
    pub path: Option<PathBuf>,
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
    /// Load and parse a configuration file from `path`, expanding any
    /// `[include] files=` globs (relative to the config file's directory).
    pub fn load(path: &std::path::Path) -> Result<Config, String> {
        let text = std::fs::read_to_string(path)
            .map_err(|e| format!("cannot read config {}: {e}", path.display()))?;
        let mut sections = parse_ini(&text)?;

        // Process [include] files=, in the directory of the main config.
        let base_dir = path.parent().unwrap_or_else(|| std::path::Path::new("."));
        let include_globs: Vec<String> = sections
            .iter()
            .filter(|s| s.name == "include")
            .flat_map(|s| {
                items_map(&s.items)
                    .get("files")
                    .map(|v| {
                        v.split_whitespace().map(|s| s.to_string()).collect::<Vec<_>>()
                    })
                    .unwrap_or_default()
            })
            .collect();
        for pattern in include_globs {
            for file in expand_glob(base_dir, &pattern) {
                let itext = std::fs::read_to_string(&file)
                    .map_err(|e| format!("cannot read included config {}: {e}", file.display()))?;
                sections.extend(parse_ini(&itext)?);
            }
        }

        let mut config = Config::build(sections)?;
        config.path = Some(path.to_path_buf());
        Ok(config)
    }

    /// Parse configuration from a single string (no `[include]` expansion).
    pub fn parse(text: &str) -> Result<Config, String> {
        Config::build(parse_ini(text)?)
    }

    /// Build a [`Config`] from already-tokenised sections.
    fn build(sections: Vec<Section>) -> Result<Config, String> {
        let mut supervisord = SupervisordConfig::default();
        let mut socket_path = None;
        let mut unix_auth = HttpAuth::default();
        let mut inet_addr = None;
        let mut inet_auth = HttpAuth::default();
        let mut programs = Vec::new();

        for sec in &sections {
            if sec.name == "supervisord" {
                supervisord = parse_supervisord(&sec.items)?;
            } else if sec.name == "unix_http_server" {
                let m = items_map(&sec.items);
                if let Some(f) = m.get("file") {
                    socket_path = Some(PathBuf::from(*f));
                }
                unix_auth = HttpAuth {
                    username: m.get("username").map(|s| s.to_string()),
                    password: m.get("password").map(|s| s.to_string()),
                };
            } else if sec.name == "inet_http_server" {
                let m = items_map(&sec.items);
                if let Some(p) = m.get("port") {
                    inet_addr = Some(normalize_inet_addr(p));
                }
                inet_auth = HttpAuth {
                    username: m.get("username").map(|s| s.to_string()),
                    password: m.get("password").map(|s| s.to_string()),
                };
            } else if let Some(prog) = sec.name.strip_prefix("program:") {
                let expanded = parse_program(prog.trim(), &sec.items, &supervisord, false)?;
                programs.extend(expanded);
            } else if let Some(listener) = sec.name.strip_prefix("eventlistener:") {
                let expanded = parse_program(listener.trim(), &sec.items, &supervisord, true)?;
                programs.extend(expanded);
            }
            // Other sections (rpcinterface, supervisorctl) are accepted but
            // ignored in this core. `group:` sections are processed below.
        }

        // Apply `[group:x]` membership: every instance of a referenced program
        // joins that group. Programs not named by any group keep their
        // homogeneous group (group == program name).
        for sec in &sections {
            if let Some(group_name) = sec.name.strip_prefix("group:") {
                let group_name = group_name.trim().to_string();
                let m = items_map(&sec.items);
                let members: Vec<String> = m
                    .get("programs")
                    .map(|v| {
                        v.split(',')
                            .map(|s| s.trim().to_string())
                            .filter(|s| !s.is_empty())
                            .collect()
                    })
                    .unwrap_or_default();
                let group_priority = m
                    .get("priority")
                    .map(|v| v.parse())
                    .transpose()
                    .map_err(|_| "invalid group priority")?;
                for member in &members {
                    let mut found = false;
                    for p in programs.iter_mut() {
                        if &p.program == member {
                            p.group = group_name.clone();
                            if let Some(prio) = group_priority {
                                p.priority = prio;
                            }
                            found = true;
                        }
                    }
                    if !found {
                        return Err(format!("[group:{group_name}] names unknown program {member}"));
                    }
                }
            }
        }

        // Order programs by priority for deterministic startup.
        programs.sort_by_key(|p| p.priority);

        Ok(Config {
            supervisord,
            programs,
            socket_path,
            unix_auth,
            inet_addr,
            inet_auth,
            path: None,
        })
    }
}

/// Expand a (possibly wildcard) include pattern against `base_dir`. Supports
/// `*` and `?` in the final path component, which covers the common
/// `conf.d/*.conf` case. Results are sorted for determinism.
fn expand_glob(base_dir: &std::path::Path, pattern: &str) -> Vec<PathBuf> {
    let joined = if std::path::Path::new(pattern).is_absolute() {
        PathBuf::from(pattern)
    } else {
        base_dir.join(pattern)
    };

    let parent = joined.parent().unwrap_or_else(|| std::path::Path::new("."));
    let file_pat = joined
        .file_name()
        .map(|s| s.to_string_lossy().into_owned())
        .unwrap_or_default();

    if !file_pat.contains('*') && !file_pat.contains('?') {
        // No wildcard: a plain file path.
        return if joined.exists() { vec![joined] } else { Vec::new() };
    }

    let mut matches = Vec::new();
    if let Ok(entries) = std::fs::read_dir(parent) {
        for entry in entries.flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if glob_match(&file_pat, &name) {
                matches.push(entry.path());
            }
        }
    }
    matches.sort();
    matches
}

/// Match a filename against a simple glob with `*` (any run) and `?` (one
/// char). No `[...]` classes — kept intentionally small.
fn glob_match(pattern: &str, name: &str) -> bool {
    let p: Vec<char> = pattern.chars().collect();
    let n: Vec<char> = name.chars().collect();
    // Classic two-pointer wildcard match with backtracking on `*`.
    let (mut pi, mut ni) = (0usize, 0usize);
    let (mut star, mut mark) = (None, 0usize);
    while ni < n.len() {
        if pi < p.len() && (p[pi] == '?' || p[pi] == n[ni]) {
            pi += 1;
            ni += 1;
        } else if pi < p.len() && p[pi] == '*' {
            star = Some(pi);
            mark = ni;
            pi += 1;
        } else if let Some(s) = star {
            pi = s + 1;
            mark += 1;
            ni = mark;
        } else {
            return false;
        }
    }
    while pi < p.len() && p[pi] == '*' {
        pi += 1;
    }
    pi == p.len()
}

/// Normalise an `[inet_http_server] port=` value into a `host:port` string
/// suitable for `TcpListener::bind`. `*:9001` binds all interfaces; a bare
/// `9001` binds localhost.
fn normalize_inet_addr(port: &str) -> String {
    let port = port.trim();
    if let Some(rest) = port.strip_prefix("*:") {
        format!("0.0.0.0:{rest}")
    } else if port.contains(':') {
        port.to_string()
    } else {
        format!("127.0.0.1:{port}")
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
    is_listener: bool,
) -> Result<Vec<ProgramConfig>, String> {
    let m = items_map(items);

    let kind = if is_listener { "eventlistener" } else { "program" };
    let command = m
        .get("command")
        .ok_or_else(|| format!("[{kind}:{name}] is missing required 'command'"))?
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
    // Listeners default to a high (low-numbered) priority so they start
    // before the programs whose events they want to observe.
    let default_priority = if is_listener { -1 } else { 999 };
    let priority = m.get("priority").map(|v| v.parse()).transpose().map_err(|_| "invalid priority")?.unwrap_or(default_priority);
    // Event listeners must keep stderr separate so it can't corrupt the
    // protocol stream on stdout.
    let redirect_stderr = if is_listener {
        false
    } else {
        m.get("redirect_stderr").map(|v| parse_bool(v)).transpose()?.unwrap_or(false)
    };

    let buffer_size = m.get("buffer_size").map(|v| v.parse()).transpose().map_err(|_| "invalid buffer_size")?.unwrap_or(10);
    let events: Vec<String> = m
        .get("events")
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_ascii_uppercase())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();

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
            program: name.to_string(),
            group: name.to_string(),
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
            is_listener,
            events: events.clone(),
            buffer_size,
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
    fn glob_matches_wildcards() {
        assert!(glob_match("*.conf", "web.conf"));
        assert!(glob_match("*.conf", ".conf"));
        assert!(!glob_match("*.conf", "web.cfg"));
        assert!(glob_match("conf-?.ini", "conf-1.ini"));
        assert!(!glob_match("conf-?.ini", "conf-12.ini"));
        assert!(glob_match("a*b*c", "axxbxxc"));
    }

    #[test]
    fn group_section_assigns_membership() {
        let text = "\
[program:a]
command=/bin/true
[program:b]
command=/bin/true
[group:grp]
programs=a,b
priority=5
";
        let cfg = Config::parse(text).unwrap();
        for p in &cfg.programs {
            assert_eq!(p.group, "grp");
            assert_eq!(p.priority, 5);
        }
    }

    #[test]
    fn eventlistener_section_is_parsed() {
        let text = "\
[eventlistener:l]
command=/bin/cat
events=PROCESS_STATE,TICK_60
buffer_size=20
";
        let cfg = Config::parse(text).unwrap();
        assert_eq!(cfg.programs.len(), 1);
        let l = &cfg.programs[0];
        assert!(l.is_listener);
        assert_eq!(l.buffer_size, 20);
        assert_eq!(l.events, vec!["PROCESS_STATE".to_string(), "TICK_60".to_string()]);
        assert_eq!(l.priority, -1); // listeners start first by default
    }

    #[test]
    fn parses_environment_pairs() {
        let env = parse_environment(r#"A="1",B="2,3""#);
        assert_eq!(env, vec![("A".into(), "1".into()), ("B".into(), "2,3".into())]);
    }
}
