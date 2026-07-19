//! Small helpers shared across modules.

/// Return the index of the first occurrence of `needle` within `haystack`,
/// or `None` if it does not appear.
pub fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
}

/// Compare two byte strings in constant time (with respect to the contents),
/// so credential checks don't leak how many leading bytes matched via timing.
/// Differing lengths return `false` immediately (the length itself is not
/// considered secret here).
pub fn constant_time_eq(a: &[u8], b: &[u8]) -> bool {
    if a.len() != b.len() {
        return false;
    }
    let mut diff = 0u8;
    for (x, y) in a.iter().zip(b.iter()) {
        diff |= x ^ y;
    }
    diff == 0
}

/// Portable `pipe2(2)`: create a pipe with the `O_CLOEXEC` / `O_NONBLOCK`
/// bits from `flags` applied to both ends. Returns 0 on success, -1 on
/// failure (with `errno` set), like the syscall.
///
/// On Linux this is the real `pipe2`, so the flags are applied atomically.
/// Other platforms (e.g. macOS) have no `pipe2`, so we fall back to
/// `pipe(2)` + `fcntl(2)`; supervisord is single-threaded at pipe-creation
/// time, so the window between the two calls is harmless there.
#[cfg(any(target_os = "linux", target_os = "android"))]
pub fn pipe2(fds: &mut [libc::c_int; 2], flags: libc::c_int) -> libc::c_int {
    unsafe { libc::pipe2(fds.as_mut_ptr(), flags) }
}

#[cfg(not(any(target_os = "linux", target_os = "android")))]
pub fn pipe2(fds: &mut [libc::c_int; 2], flags: libc::c_int) -> libc::c_int {
    unsafe {
        if libc::pipe(fds.as_mut_ptr()) != 0 {
            return -1;
        }
        for &fd in fds.iter() {
            if flags & libc::O_CLOEXEC != 0 {
                let fd_flags = libc::fcntl(fd, libc::F_GETFD);
                if fd_flags < 0
                    || libc::fcntl(fd, libc::F_SETFD, fd_flags | libc::FD_CLOEXEC) != 0
                {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                    return -1;
                }
            }
            if flags & libc::O_NONBLOCK != 0 {
                let fl_flags = libc::fcntl(fd, libc::F_GETFL);
                if fl_flags < 0 || libc::fcntl(fd, libc::F_SETFL, fl_flags | libc::O_NONBLOCK) != 0
                {
                    libc::close(fds[0]);
                    libc::close(fds[1]);
                    return -1;
                }
            }
        }
        0
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn finds_and_misses() {
        assert_eq!(find_subslice(b"hello world", b"world"), Some(6));
        assert_eq!(find_subslice(b"hello", b"x"), None);
        assert_eq!(find_subslice(b"abc", b""), None);
        assert_eq!(find_subslice(b"a", b"abc"), None);
    }

    #[test]
    fn constant_time_eq_matches_semantics() {
        assert!(constant_time_eq(b"secret", b"secret"));
        assert!(!constant_time_eq(b"secret", b"secres"));
        assert!(!constant_time_eq(b"secret", b"secre"));
        assert!(constant_time_eq(b"", b""));
        assert!(!constant_time_eq(b"a", b""));
    }
}
