//! Small helpers shared across modules.

/// Return the index of the first occurrence of `needle` within `haystack`,
/// or `None` if it does not appear.
pub fn find_subslice(haystack: &[u8], needle: &[u8]) -> Option<usize> {
    if needle.is_empty() || haystack.len() < needle.len() {
        return None;
    }
    haystack.windows(needle.len()).position(|w| w == needle)
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
}
