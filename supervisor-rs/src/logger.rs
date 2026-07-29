//! Size-based rotating log files, used both for the main `supervisord.log`
//! and for each child process's captured stdout/stderr.

use std::fs::{File, OpenOptions};
use std::io::Write;
use std::path::PathBuf;

/// A log sink that rotates when it grows past `maxbytes`.
///
/// Rotation renames `file` -> `file.1` -> `file.2` ... up to `backups`
/// files, matching the original Supervisor behaviour. A `maxbytes` of `0`
/// disables rotation (the file grows without bound). A logger with no path
/// silently discards everything written to it.
pub struct RotatingLogger {
    path: Option<PathBuf>,
    maxbytes: u64,
    backups: u32,
    file: Option<File>,
    size: u64,
}

impl RotatingLogger {
    /// Create a logger writing to `path`. `path` of `None` discards output.
    pub fn new(path: Option<PathBuf>, maxbytes: u64, backups: u32) -> std::io::Result<Self> {
        let (file, size) = match &path {
            Some(p) => {
                let f = OpenOptions::new().create(true).append(true).open(p)?;
                let size = f.metadata().map(|m| m.len()).unwrap_or(0);
                (Some(f), size)
            }
            None => (None, 0),
        };
        Ok(RotatingLogger {
            path,
            maxbytes,
            backups,
            file,
            size,
        })
    }

    /// A logger that discards everything.
    pub fn null() -> Self {
        RotatingLogger {
            path: None,
            maxbytes: 0,
            backups: 0,
            file: None,
            size: 0,
        }
    }

    /// Append `buf`, rotating first if it would exceed `maxbytes`.
    pub fn write(&mut self, buf: &[u8]) {
        if self.file.is_none() {
            return;
        }
        if self.maxbytes > 0 && self.size + buf.len() as u64 > self.maxbytes {
            self.rotate();
        }
        if let Some(f) = self.file.as_mut() {
            if f.write_all(buf).is_ok() {
                let _ = f.flush();
                self.size += buf.len() as u64;
            }
        }
    }

    /// Truncate the current log and delete its rotated backups.
    pub fn clear(&mut self) {
        let Some(path) = self.path.clone() else { return };
        self.file = None;
        for i in 1..=self.backups {
            let _ = std::fs::remove_file(backup_path(&path, i));
        }
        // Truncate the active file, then reopen for appending.
        let _ = OpenOptions::new()
            .write(true)
            .truncate(true)
            .create(true)
            .open(&path);
        self.file = OpenOptions::new().create(true).append(true).open(&path).ok();
        self.size = 0;
    }

    fn rotate(&mut self) {
        let Some(path) = self.path.clone() else { return };
        // Drop the current handle before renaming.
        self.file = None;

        if self.backups > 0 {
            // Remove the oldest, then shift each backup up by one.
            let oldest = backup_path(&path, self.backups);
            let _ = std::fs::remove_file(&oldest);
            for i in (1..self.backups).rev() {
                let src = backup_path(&path, i);
                let dst = backup_path(&path, i + 1);
                let _ = std::fs::rename(&src, &dst);
            }
            let _ = std::fs::rename(&path, backup_path(&path, 1));
        } else {
            // No backups: just truncate.
            let _ = std::fs::remove_file(&path);
        }

        if let Ok(f) = OpenOptions::new().create(true).append(true).open(&path) {
            self.file = Some(f);
        }
        self.size = 0;
    }
}

fn backup_path(path: &std::path::Path, n: u32) -> PathBuf {
    let mut s = path.as_os_str().to_os_string();
    s.push(format!(".{n}"));
    PathBuf::from(s)
}
