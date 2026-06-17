//! The control protocol spoken between `supervisorctl` and `supervisord`.
//!
//! The original Supervisor exposes an XML-RPC API over HTTP. For this core
//! reimplementation we use a deliberately simple, line-oriented protocol
//! over the same unix domain socket:
//!
//! * The client connects and writes exactly one request line terminated by
//!   `\n`: a command and optional arguments, e.g. `status`, `start web`,
//!   `stop all`.
//! * The server writes back a UTF-8 response and closes the connection. The
//!   client reads until EOF.
//!
//! This keeps the client and server trivially interoperable without pulling
//! in an HTTP/XML-RPC stack, while leaving room to add XML-RPC later.

use std::io::{Read, Write};
use std::os::unix::net::UnixStream;
use std::path::Path;

/// Send a single command to the daemon over its unix socket and return the
/// full response text.
pub fn request(socket: &Path, command: &str) -> std::io::Result<String> {
    let mut stream = UnixStream::connect(socket)?;
    stream.write_all(command.as_bytes())?;
    if !command.ends_with('\n') {
        stream.write_all(b"\n")?;
    }
    // Signal we're done writing so the server can respond and we read to EOF.
    let _ = stream.shutdown(std::net::Shutdown::Write);
    let mut response = String::new();
    stream.read_to_string(&mut response)?;
    Ok(response)
}
