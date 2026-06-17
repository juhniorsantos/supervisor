//! A tiny HTTP/1.1 server core: enough to read a single request (method,
//! path, headers, body) and write one response back over any stream. Used
//! for both the XML-RPC endpoint (`POST /RPC2`) and the web UI (`GET /`).

use std::io::{Read, Write};

/// A parsed HTTP request.
pub struct Request {
    pub method: String,
    pub path: String,
    pub headers: Vec<(String, String)>,
    pub body: String,
}

impl Request {
    pub fn header(&self, name: &str) -> Option<&str> {
        self.headers
            .iter()
            .find(|(k, _)| k.eq_ignore_ascii_case(name))
            .map(|(_, v)| v.as_str())
    }

    /// The decoded `(username, password)` from a Basic `Authorization`
    /// header, if present and well-formed.
    pub fn basic_auth(&self) -> Option<(String, String)> {
        let value = self.header("authorization")?;
        let b64 = value.strip_prefix("Basic ").or_else(|| value.strip_prefix("basic "))?;
        let decoded = base64_decode(b64.trim())?;
        let text = String::from_utf8(decoded).ok()?;
        let (u, p) = text.split_once(':')?;
        Some((u.to_string(), p.to_string()))
    }
}

/// Read a single HTTP request from `stream`. Returns `None` on EOF or a
/// malformed request.
pub fn read_request<S: Read>(stream: &mut S) -> Option<Request> {
    let mut buf: Vec<u8> = Vec::with_capacity(1024);
    let mut chunk = [0u8; 4096];

    // Read until we have the full header block.
    let header_end = loop {
        if let Some(pos) = crate::util::find_subslice(&buf, b"\r\n\r\n") {
            break pos + 4;
        }
        match stream.read(&mut chunk) {
            Ok(0) => return None,
            Ok(n) => buf.extend_from_slice(&chunk[..n]),
            Err(_) => return None,
        }
        if buf.len() > 1024 * 1024 {
            return None; // header too large
        }
    };

    let header_text = String::from_utf8_lossy(&buf[..header_end]).to_string();
    let mut lines = header_text.split("\r\n");
    let request_line = lines.next()?;
    let mut parts = request_line.split_whitespace();
    let method = parts.next()?.to_string();
    let path = parts.next()?.to_string();

    let mut headers = Vec::new();
    let mut content_length = 0usize;
    for line in lines {
        if line.is_empty() {
            continue;
        }
        if let Some((k, v)) = line.split_once(':') {
            let k = k.trim().to_string();
            let v = v.trim().to_string();
            if k.eq_ignore_ascii_case("content-length") {
                content_length = v.parse().unwrap_or(0);
            }
            headers.push((k, v));
        }
    }

    // Read the remaining body bytes.
    let mut body_bytes = buf[header_end..].to_vec();
    while body_bytes.len() < content_length {
        match stream.read(&mut chunk) {
            Ok(0) => break,
            Ok(n) => body_bytes.extend_from_slice(&chunk[..n]),
            Err(_) => break,
        }
    }
    body_bytes.truncate(content_length);
    let body = String::from_utf8_lossy(&body_bytes).to_string();

    Some(Request {
        method,
        path,
        headers,
        body,
    })
}

/// Write a complete HTTP response.
pub fn write_response<S: Write>(
    stream: &mut S,
    status: u16,
    reason: &str,
    content_type: &str,
    body: &str,
    extra_headers: &[(&str, &str)],
) {
    let mut head = format!(
        "HTTP/1.1 {status} {reason}\r\nContent-Type: {content_type}\r\nContent-Length: {}\r\nConnection: close\r\n",
        body.len()
    );
    for (k, v) in extra_headers {
        head.push_str(k);
        head.push_str(": ");
        head.push_str(v);
        head.push_str("\r\n");
    }
    head.push_str("\r\n");
    let _ = stream.write_all(head.as_bytes());
    let _ = stream.write_all(body.as_bytes());
    let _ = stream.flush();
}

/// Minimal standard base64 decoder (no external crates).
fn base64_decode(input: &str) -> Option<Vec<u8>> {
    fn val(c: u8) -> Option<u8> {
        match c {
            b'A'..=b'Z' => Some(c - b'A'),
            b'a'..=b'z' => Some(c - b'a' + 26),
            b'0'..=b'9' => Some(c - b'0' + 52),
            b'+' => Some(62),
            b'/' => Some(63),
            _ => None,
        }
    }
    let bytes: Vec<u8> = input.bytes().filter(|b| !b.is_ascii_whitespace()).collect();
    let mut out = Vec::new();
    for chunk in bytes.chunks(4) {
        let mut acc = 0u32;
        let mut count = 0;
        for &c in chunk {
            if c == b'=' {
                break;
            }
            acc = (acc << 6) | val(c)? as u32;
            count += 1;
        }
        match count {
            4 => {
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
                out.push(acc as u8);
            }
            3 => {
                acc <<= 6;
                out.push((acc >> 16) as u8);
                out.push((acc >> 8) as u8);
            }
            2 => {
                acc <<= 12;
                out.push((acc >> 16) as u8);
            }
            _ => {}
        }
    }
    Some(out)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Cursor;

    #[test]
    fn parses_a_post_with_body() {
        let raw = "POST /RPC2 HTTP/1.1\r\nHost: x\r\nContent-Length: 5\r\n\r\nhello";
        let mut cur = Cursor::new(raw.as_bytes().to_vec());
        let req = read_request(&mut cur).unwrap();
        assert_eq!(req.method, "POST");
        assert_eq!(req.path, "/RPC2");
        assert_eq!(req.body, "hello");
        assert_eq!(req.header("host"), Some("x"));
    }

    #[test]
    fn decodes_basic_auth() {
        // "user:pass" base64 == "dXNlcjpwYXNz"
        let raw = "GET / HTTP/1.1\r\nAuthorization: Basic dXNlcjpwYXNz\r\n\r\n";
        let mut cur = Cursor::new(raw.as_bytes().to_vec());
        let req = read_request(&mut cur).unwrap();
        assert_eq!(req.basic_auth(), Some(("user".into(), "pass".into())));
    }
}
