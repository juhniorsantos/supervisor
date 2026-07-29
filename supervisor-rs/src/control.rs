//! The client side of the control protocol: an XML-RPC-over-HTTP caller that
//! `supervisorctl` uses to reach `supervisord`.
//!
//! Requests are sent as `POST /RPC2` over a unix domain socket (the same
//! transport the original Supervisor uses for `unix://` server URLs), so this
//! client is wire-compatible with the daemon's HTTP endpoint.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;
use std::time::Duration;

use crate::xmlrpc::{self, Value};

/// Errors that can occur while making a control call.
#[derive(Debug)]
pub enum ClientError {
    /// Could not connect to / talk to the daemon.
    Io(std::io::Error),
    /// The server returned a malformed HTTP/XML-RPC response.
    Protocol(String),
    /// The server returned an XML-RPC fault `(code, message)`.
    Fault(i32, String),
}

impl std::fmt::Display for ClientError {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ClientError::Io(e) => write!(f, "{e}"),
            ClientError::Protocol(s) => write!(f, "protocol error: {s}"),
            ClientError::Fault(c, m) => write!(f, "fault {c}: {m}"),
        }
    }
}

/// Call a `supervisor.*` method over the unix control socket.
pub fn call(socket: &Path, method: &str, params: &[Value]) -> Result<Value, ClientError> {
    let body = xmlrpc::serialize_method_call(method, params);

    let mut stream = UnixStream::connect(socket).map_err(ClientError::Io)?;
    stream
        .set_read_timeout(Some(Duration::from_secs(30)))
        .ok();

    let request = format!(
        "POST /RPC2 HTTP/1.1\r\nHost: localhost\r\nContent-Type: text/xml\r\n\
         Content-Length: {}\r\nConnection: close\r\n\r\n{}",
        body.len(),
        body
    );
    stream
        .write_all(request.as_bytes())
        .map_err(ClientError::Io)?;
    stream.flush().ok();

    let mut raw = Vec::new();
    stream.read_to_end(&mut raw).map_err(ClientError::Io)?;
    let text = String::from_utf8_lossy(&raw);

    // Split HTTP headers from the body.
    let body = text
        .split_once("\r\n\r\n")
        .map(|(_, b)| b)
        .ok_or_else(|| ClientError::Protocol("no HTTP body in response".into()))?;

    if text.starts_with("HTTP/1.1 401") || text.starts_with("HTTP/1.0 401") {
        return Err(ClientError::Protocol("authentication required".into()));
    }

    xmlrpc::parse_method_response(body).map_err(|(c, m)| ClientError::Fault(c, m))
}
