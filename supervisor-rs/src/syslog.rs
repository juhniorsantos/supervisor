//! A minimal syslog client used for `stdout_syslog`/`stderr_syslog`.
//!
//! Messages are sent as RFC 3164-ish datagrams to the local syslog socket
//! (`/dev/log`). The implementation is deliberately small and best-effort:
//! if the socket is unavailable, writes are silently dropped (matching the
//! "logging should never crash the supervisor" principle).

use std::os::unix::net::UnixDatagram;

/// Facility `daemon` (3) × 8 + severity `info` (6) = priority 30.
const DEFAULT_PRIORITY: u8 = 30;

/// A line-buffered writer that forwards complete lines to syslog, tagged with
/// a program name.
pub struct Syslog {
    sock: Option<UnixDatagram>,
    tag: String,
    buf: Vec<u8>,
}

impl Syslog {
    /// Connect to the system log socket at `/dev/log`.
    pub fn new(tag: &str) -> Self {
        Self::connect("/dev/log", tag)
    }

    /// Connect to an explicit datagram socket path (used in tests).
    pub fn connect(path: &str, tag: &str) -> Self {
        let sock = UnixDatagram::unbound()
            .ok()
            .filter(|s| s.connect(path).is_ok());
        Syslog {
            sock,
            tag: tag.to_string(),
            buf: Vec::new(),
        }
    }

    /// Feed raw stream bytes; complete lines are sent to syslog and any
    /// trailing partial line is retained for the next call.
    pub fn feed(&mut self, data: &[u8]) {
        if self.sock.is_none() {
            return;
        }
        self.buf.extend_from_slice(data);
        while let Some(nl) = self.buf.iter().position(|&b| b == b'\n') {
            let line: Vec<u8> = self.buf.drain(..=nl).collect();
            // Drop the trailing newline before sending.
            self.send(&line[..line.len() - 1]);
        }
        // Guard against unbounded growth if a process never emits a newline.
        if self.buf.len() > 1 << 20 {
            let line = std::mem::take(&mut self.buf);
            self.send(&line);
        }
    }

    fn send(&self, line: &[u8]) {
        let Some(sock) = self.sock.as_ref() else { return };
        let mut msg = format!("<{DEFAULT_PRIORITY}>{}: ", self.tag).into_bytes();
        msg.extend_from_slice(line);
        let _ = sock.send(&msg);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn forwards_complete_lines() {
        let dir = std::env::temp_dir().join(format!("syslog-test-{}", std::process::id()));
        let _ = std::fs::remove_file(&dir);
        let server = UnixDatagram::bind(&dir).unwrap();
        server
            .set_read_timeout(Some(std::time::Duration::from_secs(2)))
            .unwrap();

        let mut sl = Syslog::connect(dir.to_str().unwrap(), "myprog");
        sl.feed(b"hello\nwor");
        sl.feed(b"ld\n");

        let mut buf = [0u8; 256];
        let n = server.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"<30>myprog: hello");
        let n = server.recv(&mut buf).unwrap();
        assert_eq!(&buf[..n], b"<30>myprog: world");

        let _ = std::fs::remove_file(&dir);
    }
}
