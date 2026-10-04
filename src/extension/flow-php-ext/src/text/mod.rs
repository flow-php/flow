//! The text of column values as the PHP writers render it (`Flow\ETL\Column\TextValues` plus each encoder's syntax),
//! read straight from the arrow arrays.

pub mod batch;
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
