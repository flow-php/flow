//! How PHP keys a column: `Schema::definitions()` holds a definition under its name, and PHP turns a canonical decimal
//! name (`"7"`, not `"07"` or `"7 "`) into the int key `7`.

/// The int PHP keys an array by when `key` is a canonical decimal integer, as `$array["7"]` is `$array[7]`.
pub fn array_key_index(key: &[u8]) -> Option<i64> {
    let string = std::str::from_utf8(key).ok()?;
    let value: i64 = string.parse().ok()?;

    (value.to_string().as_bytes() == key).then_some(value)
}

/// A `Schema::definitions()` key: an int for a numeric name, as PHP keys it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Key {
    Index(i64),
    Name(Vec<u8>),
}

impl Key {
    pub fn name(&self) -> Vec<u8> {
        match self {
            Key::Index(index) => index.to_string().into_bytes(),
            Key::Name(name) => name.clone(),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{array_key_index, Key};

    #[test]
    fn a_canonical_decimal_is_an_int_key() {
        assert_eq!(array_key_index(b"7"), Some(7));
        assert_eq!(array_key_index(b"-7"), Some(-7));
        assert_eq!(array_key_index(b"0"), Some(0));
        assert_eq!(array_key_index(b"9223372036854775807"), Some(i64::MAX));
    }

    #[test]
    fn anything_else_stays_a_string_key() {
        for name in [
            &b"07"[..],
            b"7 ",
            b" 7",
            b"+7",
            b"-0",
            b"7.0",
            b"",
            b"a",
            b"9223372036854775808",
            b"\xff",
        ] {
            assert_eq!(array_key_index(name), None, "{name:?}");
        }
    }

    #[test]
    fn a_key_names_itself_as_php_prints_it() {
        assert_eq!(Key::Index(7).name(), b"7");
        assert_eq!(Key::Name(b"07".to_vec()).name(), b"07");
    }
}
