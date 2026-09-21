/// Necessary condition for nesting deeper than json_validate's 511, not a validator: such a cell holds at least
/// 512 `[`/`{` bytes. Brackets inside strings only cost an extra PHP call.
pub fn json_nesting_within_php_depth(bytes: &[u8]) -> bool {
    memchr::memchr2_iter(b'[', b'{', bytes).take(512).count() <= 511
}

/// Necessary condition for an unpaired surrogate escape, not a validator: it needs a `\u` followed by `d8`-`df`.
/// Any such escape, paired or not, is left to PHP.
pub fn json_without_surrogate_escape(bytes: &[u8]) -> bool {
    !memchr::memchr_iter(b'\\', bytes).any(|at| {
        matches!(bytes.get(at + 1..at + 4), Some([b'u', b'd' | b'D', b'8'..=b'9' | b'a'..=b'f' | b'A'..=b'F']))
    })
}

/// `true` is json_validate()'s verdict; `false` is never authoritative - the caller asks PHP.
pub fn json_valid(bytes: &[u8]) -> bool {
    std::str::from_utf8(bytes).is_ok()
        && serde_json::from_slice::<serde::de::IgnoredAny>(bytes).is_ok()
        && json_nesting_within_php_depth(bytes)
        && json_without_surrogate_escape(bytes)
}
