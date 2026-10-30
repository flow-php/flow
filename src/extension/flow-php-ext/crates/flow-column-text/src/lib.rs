//! The text of column values as the PHP writers render it (`Flow\ETL\Values\Column\TextValues` plus each encoder's syntax),
//! read straight from the arrow arrays. No PHP types: a named time zone's transitions come in through
//! [`date::Transitions`].

pub mod date;
pub mod float;
pub mod json;
pub mod value;

/// What `json_encode()` refuses; `message()` and `code()` are `json_last_error_msg()` and `json_last_error()`.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Error {
    NonFinite,
    Utf8,
}

impl Error {
    pub fn message(self) -> &'static str {
        match self {
            Error::NonFinite => "Inf and NaN cannot be JSON encoded",
            Error::Utf8 => "Malformed UTF-8 characters, possibly incorrectly encoded",
        }
    }

    pub fn code(self) -> i32 {
        match self {
            Error::NonFinite => 7,
            Error::Utf8 => 5,
        }
    }
}

/// A decimal integer, as `(string) $int` writes it.
pub fn int(out: &mut Vec<u8>, value: i64) {
    let mut buffer = [0u8; 20];
    let mut at = buffer.len();
    let mut rest = value.unsigned_abs();

    loop {
        at -= 1;
        buffer[at] = b'0' + (rest % 10) as u8;
        rest /= 10;

        if rest == 0 {
            break;
        }
    }

    if value < 0 {
        out.push(b'-');
    }

    out.extend_from_slice(&buffer[at..]);
}

/// The lowercase 36-char form of 16 uuid bytes, appended to `out`.
pub fn uuid(out: &mut Vec<u8>, bytes: &[u8]) {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            out.push(b'-');
        }

        out.push(HEX[usize::from(byte >> 4)]);
        out.push(HEX[usize::from(byte & 0x0f)]);
    }
}

#[cfg(test)]
mod tests {
    use super::{int, uuid};

    fn text(write: impl FnOnce(&mut Vec<u8>)) -> String {
        let mut out = Vec::new();

        write(&mut out);

        String::from_utf8(out).unwrap()
    }

    #[test]
    fn an_int_is_written_as_php_casts_it() {
        assert_eq!(text(|out| int(out, 0)), "0");
        assert_eq!(text(|out| int(out, -7)), "-7");
        assert_eq!(text(|out| int(out, i64::MAX)), "9223372036854775807");
        assert_eq!(text(|out| int(out, i64::MIN)), "-9223372036854775808");
    }

    #[test]
    fn a_uuid_is_its_lowercase_hyphenated_form() {
        assert_eq!(
            text(|out| uuid(
                out,
                &[0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0x4d, 0xef, 0x81, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef]
            )),
            "01234567-89ab-4def-8123-456789abcdef"
        );
    }
}
