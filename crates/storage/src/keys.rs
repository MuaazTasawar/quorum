/// The longest key, in bytes of UTF-8, the store accepts. Keys are carried in every log
/// entry that touches them and in every URL that addresses them, so they are kept short.
pub const MAX_KEY_BYTES: usize = 512;

/// Why a key was refused.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum KeyError {
    #[error("a key must not be empty")]
    Empty,
    #[error("a key may be at most {max} bytes, this one is {len}")]
    TooLong { len: usize, max: usize },
    #[error("a key must not contain control characters (found one at byte {at})")]
    ControlCharacter { at: usize },
    /// `.` and `..` mean "this directory" and "the parent" to every URL parser, even
    /// when percent-encoded, so a key with either name could never be addressed over
    /// HTTP: a request for it would silently become a request for no key at all.
    #[error("{0:?} is reserved and cannot be used as a key")]
    Reserved(String),
}

/// Checks a key against the rules every part of the system applies: non-empty, at most
/// [`MAX_KEY_BYTES`] bytes, no control characters (NUL, newlines, escapes and the
/// like, which are invisible in logs and break line-oriented tooling), and not `.` or
/// `..`. Anything else, including spaces, slashes, dots inside a longer name and any
/// Unicode text, is a valid key.
///
/// The server enforces this on every request and the client checks it first, so a
/// bad key costs no round trip.
pub fn validate_key(key: &str) -> Result<(), KeyError> {
    if key.is_empty() {
        return Err(KeyError::Empty);
    }
    if key == "." || key == ".." {
        return Err(KeyError::Reserved(key.to_string()));
    }
    if key.len() > MAX_KEY_BYTES {
        return Err(KeyError::TooLong { len: key.len(), max: MAX_KEY_BYTES });
    }
    if let Some((at, _)) = key.char_indices().find(|(_, c)| c.is_control()) {
        return Err(KeyError::ControlCharacter { at });
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn ordinary_keys_are_valid() {
        for key in ["a", "user:42", "path/with/slashes", "with space", "q?x=1&y=2#frag", "100%", "ключ", "鍵🔑"] {
            assert_eq!(validate_key(key), Ok(()), "{key:?}");
        }
    }

    #[test]
    fn an_empty_key_is_refused() {
        assert_eq!(validate_key(""), Err(KeyError::Empty));
    }

    #[test]
    fn the_length_limit_counts_bytes_not_characters() {
        assert_eq!(validate_key(&"a".repeat(MAX_KEY_BYTES)), Ok(()));
        assert_eq!(
            validate_key(&"a".repeat(MAX_KEY_BYTES + 1)),
            Err(KeyError::TooLong { len: MAX_KEY_BYTES + 1, max: MAX_KEY_BYTES })
        );
        // 256 two-byte characters fit exactly; one more does not.
        assert_eq!(validate_key(&"é".repeat(256)), Ok(()));
        assert!(matches!(validate_key(&"é".repeat(257)), Err(KeyError::TooLong { .. })));
    }

    #[test]
    fn control_characters_are_refused_wherever_they_are() {
        assert_eq!(validate_key("a\0b"), Err(KeyError::ControlCharacter { at: 1 }));
        assert_eq!(validate_key("line\nbreak"), Err(KeyError::ControlCharacter { at: 4 }));
        assert_eq!(validate_key("\u{7f}"), Err(KeyError::ControlCharacter { at: 0 }));
        assert!(validate_key("tab\there").is_err());
        assert!(validate_key("esc\u{1b}[0m").is_err());
        assert!(validate_key("c1\u{85}control").is_err(), "C1 controls too");
    }

    #[test]
    fn the_two_dot_names_are_reserved_but_longer_names_with_dots_are_fine() {
        assert_eq!(validate_key("."), Err(KeyError::Reserved(".".into())));
        assert_eq!(validate_key(".."), Err(KeyError::Reserved("..".into())));
        for ok in ["...", "a.b", ".hidden", "..x", "x..", "file.txt", "a/../b"] {
            assert_eq!(validate_key(ok), Ok(()), "{ok:?}");
        }
    }
}
