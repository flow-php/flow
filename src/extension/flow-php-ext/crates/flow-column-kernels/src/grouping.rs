//! The bucket of a canonical key, as PHP's `KeyEncoder::bucketOf()` reads it.

use xxhash_rust::xxh3::xxh3_128;

/// `(int) hexdec(substr(hash('xxh128', $key), $offset, 8)) % $buckets`
pub fn bucket_of(key: &[u8], hash_offset: u32, buckets: u64) -> u64 {
    let digits = (xxh3_128(key) >> (128 - 4 * (hash_offset + 8))) & 0xFFFF_FFFF;

    digits as u64 % buckets
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn buckets_are_the_digits_php_reads_from_the_xxh128_hex() {
        let cases: [(&[u8], u32, u64, u64); 12] = [
            (b"\0i1", 0, 2_087_460_912, 5),
            (b"\0i1", 8, 2_047_681_825, 0),
            (b"\0i1", 24, 653_081_150, 1),
            (b"\0s3:abc", 0, 3_864_089_514, 5),
            (b"\0s3:abc", 8, 2_786_302_190, 0),
            (b"\0s3:abc", 24, 1_493_301_728, 2),
            (b"\0n\0t", 0, 3_378_568_137, 0),
            (b"\0n\0t", 8, 1_143_071_435, 2),
            (b"\0n\0t", 24, 2_309_287_573, 5),
            (b"", 0, 2_578_056_915, 0),
            (b"", 8, 21_469_400, 1),
            (b"", 24, 1_183_664_511, 1),
        ];

        for (key, offset, digits, of_seven) in cases {
            assert_eq!(bucket_of(key, offset, 1 << 32), digits, "{key:?} at {offset}");
            assert_eq!(bucket_of(key, offset, 7), of_seven, "{key:?} at {offset} of 7");
        }
    }
}
