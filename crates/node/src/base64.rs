//! Standard base64 (RFC 4648 section 4, with padding), for carrying binary values
//! inside JSON. Decoding is strict: anything that is not exactly canonical base64 is
//! refused, so a value is never silently altered on the way in.

const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

pub fn encode(data: &[u8]) -> String {
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = (u32::from(b[0]) << 16) | (u32::from(b[1]) << 8) | u32::from(b[2]);
        out.push(ALPHABET[(n >> 18) as usize & 63] as char);
        out.push(ALPHABET[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { ALPHABET[(n >> 6) as usize & 63] as char } else { '=' });
        out.push(if chunk.len() > 2 { ALPHABET[n as usize & 63] as char } else { '=' });
    }
    out
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("the length of base64 text must be a multiple of 4")]
    BadLength,
    #[error("invalid base64 character at position {0}")]
    BadCharacter(usize),
    #[error("misplaced or non-canonical base64 padding")]
    BadPadding,
}

fn sextet(byte: u8) -> Option<u32> {
    match byte {
        b'A'..=b'Z' => Some(u32::from(byte - b'A')),
        b'a'..=b'z' => Some(u32::from(byte - b'a') + 26),
        b'0'..=b'9' => Some(u32::from(byte - b'0') + 52),
        b'+' => Some(62),
        b'/' => Some(63),
        _ => None,
    }
}

pub fn decode(text: &str) -> Result<Vec<u8>, DecodeError> {
    let bytes = text.as_bytes();
    if bytes.len() % 4 != 0 {
        return Err(DecodeError::BadLength);
    }
    let mut out = Vec::with_capacity(bytes.len() / 4 * 3);
    let groups = bytes.len() / 4;
    for (group_index, group) in bytes.chunks(4).enumerate() {
        let last = group_index + 1 == groups;
        let padding = group.iter().rev().take_while(|&&b| b == b'=').count();
        if padding > 2 || (padding > 0 && !last) {
            return Err(DecodeError::BadPadding);
        }
        let mut n = 0u32;
        for (i, &byte) in group.iter().enumerate() {
            let value = if i >= 4 - padding {
                0
            } else {
                sextet(byte).ok_or(DecodeError::BadCharacter(group_index * 4 + i))?
            };
            n = (n << 6) | value;
        }
        // The bits the padding leaves unused must be zero, or two different texts
        // would decode to the same bytes.
        if (padding == 1 && n & 0xFF != 0) || (padding == 2 && n & 0xFFFF != 0) {
            return Err(DecodeError::BadPadding);
        }
        out.push((n >> 16) as u8);
        if padding < 2 {
            out.push((n >> 8) as u8);
        }
        if padding < 1 {
            out.push(n as u8);
        }
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The test vectors of RFC 4648 section 10.
    const VECTORS: [(&str, &str); 7] = [
        ("", ""),
        ("f", "Zg=="),
        ("fo", "Zm8="),
        ("foo", "Zm9v"),
        ("foob", "Zm9vYg=="),
        ("fooba", "Zm9vYmE="),
        ("foobar", "Zm9vYmFy"),
    ];

    #[test]
    fn it_matches_the_rfc_vectors_both_ways() {
        for (plain, encoded) in VECTORS {
            assert_eq!(encode(plain.as_bytes()), encoded, "encoding {plain:?}");
            assert_eq!(decode(encoded).unwrap(), plain.as_bytes(), "decoding {encoded:?}");
        }
    }

    #[test]
    fn every_byte_value_survives_a_round_trip() {
        let all: Vec<u8> = (0..=255u8).collect();
        assert_eq!(decode(&encode(&all)).unwrap(), all);
        for len in 0..40 {
            let data: Vec<u8> = (0..len).map(|i| (i * 37 + 11) as u8).collect();
            assert_eq!(decode(&encode(&data)).unwrap(), data, "length {len}");
        }
    }

    #[test]
    fn the_plus_and_slash_characters_are_used() {
        assert_eq!(encode(&[0xFB, 0xFF, 0xBF]), "+/+/");
        assert_eq!(decode("+/+/").unwrap(), vec![0xFB, 0xFF, 0xBF]);
    }

    #[test]
    fn text_of_the_wrong_length_is_refused() {
        assert_eq!(decode("Zg="), Err(DecodeError::BadLength));
        assert_eq!(decode("Zm9"), Err(DecodeError::BadLength));
    }

    #[test]
    fn characters_outside_the_alphabet_are_refused_with_their_position() {
        assert_eq!(decode("Zm9!"), Err(DecodeError::BadCharacter(3)));
        assert_eq!(decode("Zm 9"), Err(DecodeError::BadCharacter(2)));
        assert_eq!(decode("Zm9v\n"), Err(DecodeError::BadLength));
        assert_eq!(decode("-_-_"), Err(DecodeError::BadCharacter(0)), "URL-safe is not standard");
    }

    #[test]
    fn padding_must_be_exactly_where_it_belongs() {
        assert_eq!(decode("Z==="), Err(DecodeError::BadPadding));
        assert_eq!(decode("Zg==Zg=="), Err(DecodeError::BadPadding), "padding in the middle");
        assert_eq!(decode("=Zg="), Err(DecodeError::BadCharacter(0)), "a leading = is not a character");
    }

    #[test]
    fn non_canonical_trailing_bits_are_refused() {
        // "Zh==" would also decode to "f" if the spare bits were ignored.
        assert_eq!(decode("Zh=="), Err(DecodeError::BadPadding));
        assert_eq!(decode("Zm9="), Err(DecodeError::BadPadding), "spare bits are not zero");
        assert_eq!(decode("Zm8=").unwrap(), b"fo");
    }
}
