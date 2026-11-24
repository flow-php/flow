//! Full lowercase hex digests of byte strings, character for character those of PHP's `hash()`.

use std::fmt::Write;

use arrow_array::LargeBinaryArray;
use md5::Md5;
use sha2::{Digest, Sha256};
use xxhash_rust::xxh3::xxh3_128;

pub type HexDigest = fn(&[u8]) -> String;

pub fn xxh128_hex(value: &[u8]) -> String {
    format!("{:032x}", xxh3_128(value))
}

pub fn md5_hex(value: &[u8]) -> String {
    hex(&Md5::digest(value))
}

pub fn sha256_hex(value: &[u8]) -> String {
    hex(&Sha256::digest(value))
}

pub fn hex_digests(values: &LargeBinaryArray, digest: HexDigest) -> LargeBinaryArray {
    values.iter().map(|value| value.map(digest)).collect()
}

pub(crate) fn hex(bytes: &[u8]) -> String {
    let mut out = String::with_capacity(bytes.len() * 2);

    for byte in bytes {
        let _ = write!(out, "{byte:02x}");
    }

    out
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::Array;

    #[test]
    fn xxh128_hex_matches_php() {
        assert_eq!(xxh128_hex(b"test"), "6c78e0e3bd51d358d01e758642b85fb8");
        assert_eq!(xxh128_hex(b""), "99aa06d3014798d86001c324468d497f");
    }

    #[test]
    fn md5_hex_matches_php() {
        assert_eq!(md5_hex(b"test"), "098f6bcd4621d373cade4e832627b4f6");
        assert_eq!(md5_hex(b""), "d41d8cd98f00b204e9800998ecf8427e");
    }

    #[test]
    fn sha256_hex_matches_php() {
        assert_eq!(
            sha256_hex(b"test"),
            "9f86d081884c7d659a2feaa0c55ad015a3bf4f1b2b0b822cd15d6c15b0f00a08"
        );
        assert_eq!(
            sha256_hex(b""),
            "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        );
    }

    #[test]
    fn hex_digests_keeps_nulls() {
        let values = LargeBinaryArray::from(vec![Some(b"test".as_slice()), None, Some(b"".as_slice())]);

        let digests = hex_digests(&values, md5_hex);

        assert_eq!(digests.len(), 3);
        assert_eq!(digests.value(0), b"098f6bcd4621d373cade4e832627b4f6");
        assert!(digests.is_null(1));
        assert_eq!(digests.value(2), b"d41d8cd98f00b204e9800998ecf8427e");
    }
}
