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
