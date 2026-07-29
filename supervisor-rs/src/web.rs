//! A minimal web management UI served at `GET /`.
//!
//! It renders a status table and exposes start/stop/restart actions as small
//! form `POST`s (so a `GET`/refresh/prefetch never changes state). It is
//! intentionally dependency-free: a single self-contained HTML page with
//! inline CSS.

use std::time::Instant;

use crate::daemon::Supervisor;
use crate::states::ProcessState;

/// Parse an `application/x-www-form-urlencoded` body (or URL query string)
/// into key/value pairs.
pub fn parse_form(body: &str) -> Vec<(String, String)> {
    parse_query(body)
}

/// Render the status page. Any action is taken from `params`
/// (`action=start|stop|restart|startall|stopall|restartall`, `name=<process>`)
/// — these come only from POST submissions, never from a GET, so the page is
/// safe to prefetch/refresh.
pub fn render(sup: &mut Supervisor, params: &[(String, String)], now: Instant) -> String {
    let mut notice = String::new();
    if let Some(action) = params.iter().find(|(k, _)| k == "action").map(|(_, v)| v.clone()) {
        let name = params
            .iter()
            .find(|(k, _)| k == "name")
            .map(|(_, v)| v.clone())
            .unwrap_or_default();
        notice = apply_action(sup, &action, &name, now);
    }

    let infos = sup.all_process_info();
    let mut rows = String::new();
    for info in &infos {
        let state = state_from_code(info.state);
        let css = state_class(state);
        let namespec = if info.group == info.name || info.group.is_empty() {
            info.name.clone()
        } else {
            format!("{}:{}", info.group, info.name)
        };
        rows.push_str(&format!(
            "<tr>\
               <td><span class=\"state {css}\">{statename}</span></td>\
               <td class=\"name\">{display}</td>\
               <td class=\"desc\">{desc}</td>\
               <td class=\"actions\">{start}{stop}{restart}</td>\
             </tr>",
            css = css,
            statename = html_escape(&info.statename),
            display = html_escape(&namespec),
            desc = html_escape(&info.description),
            start = action_button("start", &info.name, "start"),
            stop = action_button("stop", &info.name, "stop"),
            restart = action_button("restart", &info.name, "restart"),
        ));
    }
    if rows.is_empty() {
        rows.push_str("<tr><td colspan=\"4\">No programs configured</td></tr>");
    }

    let notice_html = if notice.is_empty() {
        String::new()
    } else {
        format!("<p class=\"notice\">{}</p>", html_escape(&notice))
    };

    format!(
        "<!doctype html>\n<html lang=\"en\"><head><meta charset=\"utf-8\">\
         <meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\
         <title>Supervisor Status — {ident}</title>{css}</head>\
         <body>\
           <header><h1>supervisor</h1><span class=\"ident\">{ident}</span></header>\
           {notice}\
           <div class=\"toolbar\">\
             {start_all}{stop_all}{restart_all}\
             <a class=\"btn ghost\" href=\"/\">Refresh</a>\
           </div>\
           <table><thead><tr><th>State</th><th>Name</th><th>Description</th><th>Action</th></tr></thead>\
           <tbody>{rows}</tbody></table>\
           <footer>supervisor-rs {ver} · XML-RPC at <code>POST /RPC2</code></footer>\
         </body></html>",
        ident = html_escape(sup.identifier()),
        css = STYLE,
        notice = notice_html,
        rows = rows,
        ver = env!("CARGO_PKG_VERSION"),
        start_all = toolbar_button("startall", "Start all"),
        stop_all = toolbar_button("stopall", "Stop all"),
        restart_all = toolbar_button("restartall", "Restart all"),
    )
}

/// A small POST form rendering a single per-process action button.
fn action_button(action: &str, name: &str, label: &str) -> String {
    format!(
        "<form method=\"post\" action=\"/\" class=\"act\">\
           <input type=\"hidden\" name=\"action\" value=\"{action}\">\
           <input type=\"hidden\" name=\"name\" value=\"{name}\">\
           <button type=\"submit\">{label}</button>\
         </form>",
        action = html_escape(action),
        name = html_escape(name),
        label = html_escape(label),
    )
}

/// A toolbar-wide POST action button (start/stop/restart all).
fn toolbar_button(action: &str, label: &str) -> String {
    format!(
        "<form method=\"post\" action=\"/\" class=\"tb\">\
           <input type=\"hidden\" name=\"action\" value=\"{action}\">\
           <button type=\"submit\" class=\"btn\">{label}</button>\
         </form>",
        action = html_escape(action),
        label = html_escape(label),
    )
}

fn apply_action(sup: &mut Supervisor, action: &str, name: &str, now: Instant) -> String {
    match action {
        "start" => match sup.op_start(name, now) {
            Ok(()) => format!("{name}: started"),
            Err((_, _)) => format!("{name}: could not start"),
        },
        "stop" => match sup.op_stop(name, now) {
            Ok(()) => format!("{name}: stopped"),
            Err((_, _)) => format!("{name}: not running"),
        },
        "restart" => match sup.op_restart(name, now) {
            Ok(()) => format!("{name}: restarted"),
            Err((_, _)) => format!("{name}: no such process"),
        },
        "startall" => {
            sup.op_start_all(now);
            "started all processes".to_string()
        }
        "stopall" => {
            sup.op_stop_all(now);
            "stopped all processes".to_string()
        }
        "restartall" => {
            let names: Vec<String> = sup.all_process_info().iter().map(|i| i.name.clone()).collect();
            for n in names {
                let _ = sup.op_restart(&n, now);
            }
            "restarted all processes".to_string()
        }
        _ => String::new(),
    }
}

fn state_from_code(code: i64) -> ProcessState {
    match code {
        0 => ProcessState::Stopped,
        10 => ProcessState::Starting,
        20 => ProcessState::Running,
        30 => ProcessState::Backoff,
        40 => ProcessState::Stopping,
        100 => ProcessState::Exited,
        200 => ProcessState::Fatal,
        _ => ProcessState::Unknown,
    }
}

fn state_class(state: ProcessState) -> &'static str {
    match state {
        ProcessState::Running => "ok",
        ProcessState::Starting | ProcessState::Stopping => "pending",
        ProcessState::Backoff | ProcessState::Fatal => "bad",
        _ => "off",
    }
}

/// Parse a URL query string into key/value pairs (with minimal percent and
/// `+` decoding).
fn parse_query(query: &str) -> Vec<(String, String)> {
    query
        .split('&')
        .filter(|s| !s.is_empty())
        .map(|pair| {
            let (k, v) = pair.split_once('=').unwrap_or((pair, ""));
            (url_decode(k), url_decode(v))
        })
        .collect()
}

fn url_decode(s: &str) -> String {
    let bytes = s.as_bytes();
    let mut out = Vec::with_capacity(bytes.len());
    let mut i = 0;
    while i < bytes.len() {
        match bytes[i] {
            b'+' => out.push(b' '),
            b'%' if i + 2 < bytes.len() => {
                let hi = (bytes[i + 1] as char).to_digit(16);
                let lo = (bytes[i + 2] as char).to_digit(16);
                if let (Some(h), Some(l)) = (hi, lo) {
                    out.push((h * 16 + l) as u8);
                    i += 2;
                } else {
                    out.push(bytes[i]);
                }
            }
            b => out.push(b),
        }
        i += 1;
    }
    String::from_utf8_lossy(&out).into_owned()
}

fn html_escape(s: &str) -> String {
    s.replace('&', "&amp;")
        .replace('<', "&lt;")
        .replace('>', "&gt;")
        .replace('"', "&quot;")
}

const STYLE: &str = "<style>\
:root{color-scheme:light dark}\
body{font-family:system-ui,-apple-system,Segoe UI,Roboto,sans-serif;margin:0;background:#0f1115;color:#e6e6e6}\
header{display:flex;align-items:baseline;gap:.75rem;padding:1rem 1.5rem;background:#171a21;border-bottom:1px solid #262b36}\
h1{font-size:1.25rem;margin:0;color:#fff}\
.ident{color:#8a93a6;font-size:.9rem}\
.toolbar{padding:1rem 1.5rem;display:flex;gap:.5rem;flex-wrap:wrap}\
.btn{padding:.4rem .8rem;border-radius:.4rem;background:#2a64ff;color:#fff;text-decoration:none;font-size:.85rem}\
.btn.ghost{background:#262b36}\
.notice{margin:0;padding:.6rem 1.5rem;background:#1c2a16;color:#bdf0a0;border-bottom:1px solid #2c3a22}\
table{width:100%;border-collapse:collapse}\
th,td{text-align:left;padding:.6rem 1.5rem;border-bottom:1px solid #1d222c;font-size:.9rem}\
th{color:#8a93a6;font-weight:600;font-size:.75rem;text-transform:uppercase;letter-spacing:.05em}\
.name{font-weight:600;color:#fff}\
.desc{color:#8a93a6}\
.actions{display:flex;gap:.5rem}\
.act{display:inline;margin:0}\
.act button{background:none;border:none;color:#7aa2ff;cursor:pointer;font-size:.85rem;padding:0;font-family:inherit}\
.act button:hover{text-decoration:underline}\
.toolbar form{margin:0;display:inline}\
.toolbar button{cursor:pointer;font-family:inherit}\
.state{display:inline-block;min-width:5.5rem;padding:.15rem .5rem;border-radius:.3rem;font-size:.75rem;font-weight:700;text-align:center}\
.state.ok{background:#10331c;color:#5fe08a}\
.state.bad{background:#3a1414;color:#ff8a8a}\
.state.pending{background:#33300f;color:#f0d65f}\
.state.off{background:#23262e;color:#9aa3b2}\
footer{padding:1.5rem;color:#5b6577;font-size:.8rem}\
code{background:#171a21;padding:.1rem .3rem;border-radius:.2rem}\
</style>";
