//! PHP 8's `_is_numeric_string_ex(str, len, &lval, &dval, allow_errors = false, NULL, NULL)` as Rust: the grammar
//! behind `is_numeric()`, `(int)` and `(float)` of a string. The function body is identical from PHP 8.1 to master, so
//! one mirror serves every PHP version flow_php builds against. The engine reads a NUL-terminated zend_string; this
//! mirror reads a slice, so a CSV cell is checked in place instead of being copied NUL-terminated first.

use std::cmp::Ordering;

/// `IS_LONG` / `IS_DOUBLE` from zend_types.h - the codes `_is_numeric_string_ex` returns.
pub const IS_LONG: u8 = 4;
pub const IS_DOUBLE: u8 = 5;

/// The text of `i64::MIN` without its sign; a 19-digit integer is compared against it as text, as the engine does.
const LONG_MIN_DIGITS: &[u8] = b"9223372036854775808";

/// Significant digits (leading zeros skipped) an integer may have and still be read as a long before the compare.
const MAX_LENGTH_OF_LONG: usize = 20;

pub enum Numeric {
    Long(i64),
    Double(f64),
}

/// The engine's whitespace set, which is not `is_ascii_whitespace()`: `\v` is in, and nothing non-ASCII is.
fn is_space(byte: u8) -> bool {
    matches!(byte, b' ' | b'\t' | b'\n' | b'\r' | 0x0B | 0x0C)
}

fn digits_from(bytes: &[u8], at: usize) -> usize {
    bytes[at..].iter().take_while(|byte| byte.is_ascii_digit()).count()
}

/// `[eE][+-]?[0-9]+` at `at`: the bytes it spans, or `None` when `at` is not a complete exponent.
fn exponent_len(bytes: &[u8], at: usize) -> Option<usize> {
    if !matches!(bytes.get(at), Some(b'e' | b'E')) {
        return None;
    }

    let sign = usize::from(matches!(bytes.get(at + 1), Some(b'+' | b'-')));
    let digits = digits_from(bytes, at + 1 + sign);

    (digits > 0).then_some(1 + sign + digits)
}

/// `Some` exactly when PHP's `is_numeric($s)` is true; the value is what `(int) $s` / `(float) $s` yields.
pub fn parse(bytes: &[u8]) -> Option<Numeric> {
    let start = bytes.iter().position(|byte| !is_space(*byte))?;
    let end = bytes.iter().rposition(|byte| !is_space(*byte))? + 1;
    let text = &bytes[start..end];

    let negative = text[0] == b'-';
    let unsigned = usize::from(matches!(text[0], b'+' | b'-'));

    let integer_digits = digits_from(text, unsigned);
    let mut at = unsigned + integer_digits;

    if integer_digits == 0 && !(text.get(at) == Some(&b'.') && text.get(at + 1).is_some_and(u8::is_ascii_digit)) {
        return None;
    }

    let mut floating = false;

    if text.get(at) == Some(&b'.') {
        floating = true;
        at += 1 + digits_from(text, at + 1);
    }

    if let Some(len) = exponent_len(text, at) {
        floating = true;
        at += len;
    }

    if at != text.len() {
        return None;
    }

    if floating {
        return Some(Numeric::Double(double_of(text)));
    }

    let first_significant = unsigned + text[unsigned..].iter().take_while(|byte| **byte == b'0').count();
    let significant = &text[first_significant..];

    if significant.len() >= MAX_LENGTH_OF_LONG {
        return Some(Numeric::Double(double_of(text)));
    }

    if significant.len() == MAX_LENGTH_OF_LONG - 1 {
        // the engine strcmp()s from the first significant digit to the terminator, so trailing whitespace takes part
        match bytes[start + first_significant..].cmp(LONG_MIN_DIGITS) {
            Ordering::Less => {}
            Ordering::Equal if negative => return Some(Numeric::Long(i64::MIN)),
            _ => return Some(Numeric::Double(double_of(text))),
        }
    }

    let magnitude = significant
        .iter()
        .fold(0u64, |value, digit| value * 10 + u64::from(digit - b'0'));
    let value = magnitude as i64;

    Some(Numeric::Long(if negative { value.wrapping_neg() } else { value }))
}

/// `zend_strtod` of validated text: both it and `f64::from_str` are correctly rounded, so the bits agree.
fn double_of(text: &[u8]) -> f64 {
    std::str::from_utf8(text)
        .ok()
        .and_then(|text| text.parse().ok())
        .expect("validated numeric text parses as f64")
}

#[cfg(test)]
mod tests {
    use super::{parse, Numeric};

    fn long(bytes: &[u8]) -> i64 {
        match parse(bytes) {
            Some(Numeric::Long(value)) => value,
            Some(Numeric::Double(value)) => panic!("{bytes:?} is a double {value}"),
            None => panic!("{bytes:?} is not numeric"),
        }
    }

    fn double(bytes: &[u8]) -> f64 {
        match parse(bytes) {
            Some(Numeric::Double(value)) => value,
            Some(Numeric::Long(value)) => panic!("{bytes:?} is a long {value}"),
            None => panic!("{bytes:?} is not numeric"),
        }
    }

    fn refused(bytes: &[u8]) -> bool {
        parse(bytes).is_none()
    }

    #[test]
    fn leading_and_trailing_whitespace_is_the_engine_set() {
        for space in [b" ".as_slice(), b"\t", b"\n", b"\r", b"\x0B", b"\x0C"] {
            assert_eq!(long(&[space, b"7"].concat()), 7);
            assert_eq!(long(&[b"7", space].concat()), 7);
            assert_eq!(long(&[space, b"7", space, space].concat()), 7);
        }

        assert!(refused(b"\xA07"));
        assert!(refused(b"7\xA0"));
    }

    #[test]
    fn empty_whitespace_only_and_sign_only_are_not_numeric() {
        assert!(refused(b""));
        assert!(refused(b" "));
        assert!(refused(b" \t\n"));
        assert!(refused(b"+"));
        assert!(refused(b"-"));
        assert!(refused(b" - "));
    }

    #[test]
    fn one_optional_sign() {
        assert_eq!(long(b"+5"), 5);
        assert_eq!(long(b"-5"), -5);
        assert!(refused(b"+-5"));
        assert!(refused(b"--5"));
        assert!(refused(b"5-"));
    }

    #[test]
    fn leading_zeros_are_skipped_before_significant_digits_are_counted() {
        assert_eq!(long(b"0000000000000000001"), 1);
        assert_eq!(long(b"00000000000000000000000000001"), 1);
        assert_eq!(long(b"00"), 0);
        assert_eq!(long(b"0"), 0);
        assert_eq!(double(b"00009223372036854775808"), 9223372036854775808.0);
    }

    #[test]
    fn up_to_eighteen_significant_digits_is_a_long() {
        assert_eq!(long(b"999999999999999999"), 999_999_999_999_999_999);
        assert_eq!(long(b"-999999999999999999"), -999_999_999_999_999_999);
    }

    #[test]
    fn nineteen_digits_compare_as_text_against_the_long_minimum() {
        assert_eq!(long(b"9223372036854775807"), i64::MAX);
        assert_eq!(long(b"-9223372036854775807"), -i64::MAX);
        assert_eq!(long(b"-9223372036854775808"), i64::MIN);
        assert_eq!(long(b"1000000000000000000"), 1_000_000_000_000_000_000);
        assert_eq!(double(b"9223372036854775808"), 9223372036854775808.0);
        assert_eq!(double(b"9223372036854775809"), 9223372036854775808.0);
        assert_eq!(double(b"-9223372036854775809"), -9223372036854775808.0);
    }

    #[test]
    fn trailing_bytes_take_part_in_the_nineteen_digit_compare() {
        assert_eq!(long(b"-9223372036854775808"), i64::MIN);
        assert_eq!(double(b"-9223372036854775808 "), -9223372036854775808.0);
        assert_eq!(long(b"9223372036854775807 "), i64::MAX);
        assert_eq!(double(b"9223372036854775808 "), 9223372036854775808.0);
    }

    #[test]
    fn twenty_or_more_significant_digits_is_a_double() {
        assert_eq!(double(b"10000000000000000000"), 1e19);
        assert_eq!(double(b"-10000000000000000000"), -1e19);
        assert_eq!(double(b"123456789012345678901234567890"), 1.2345678901234568e29);
    }

    #[test]
    fn a_point_or_an_exponent_makes_a_double() {
        assert_eq!(double(b"1."), 1.0);
        assert_eq!(double(b".5"), 0.5);
        assert_eq!(double(b"-.5"), -0.5);
        assert_eq!(double(b"+.5e-3"), 0.0005);
        assert_eq!(double(b"1.e5"), 100000.0);
        assert_eq!(double(b"1E-5"), 0.00001);
        assert_eq!(double(b"1e5"), 100000.0);
        assert_eq!(double(b"1.5"), 1.5);
        assert_eq!(double(b"01.5"), 1.5);
    }

    #[test]
    fn an_incomplete_point_or_exponent_is_not_numeric() {
        assert!(refused(b"."));
        assert!(refused(b".e1"));
        assert!(refused(b"1e"));
        assert!(refused(b"1e+"));
        assert!(refused(b"1e-"));
        assert!(refused(b"1e5.5"));
        assert!(refused(b"1.5.5"));
        assert!(refused(b"1e5e5"));
        assert!(refused(b"1ex"));
        assert!(refused(b"-."));
    }

    #[test]
    fn double_values_overflow_to_infinity_and_underflow_to_zero_as_the_engine_does() {
        assert_eq!(double(b"1e309"), f64::INFINITY);
        assert_eq!(double(b"-1e309"), f64::NEG_INFINITY);
        assert_eq!(double(b"1e-400"), 0.0);
        assert_eq!(double(b"4.9406564584124654e-324"), 5e-324);
        assert_eq!(double(b"2.2250738585072011e-308").to_bits(), 0x000F_FFFF_FFFF_FFFF);
        assert_eq!(double(b"9007199254740993.0"), 9007199254740992.0);
        assert_eq!(double(b"9007199254740993e0"), 9007199254740992.0);

        let long_mantissa = [b"1".as_slice(), &[b'0'; 800]].concat();
        assert_eq!(double(&long_mantissa), f64::INFINITY);
    }

    #[test]
    fn hex_underscores_named_floats_and_non_ascii_bytes_are_not_numeric() {
        assert!(refused(b"0x1A"));
        assert!(refused(b"1_000"));
        assert!(refused(b"inf"));
        assert!(refused(b"INF"));
        assert!(refused(b"-INF"));
        assert!(refused(b"nan"));
        assert!(refused(b"NAN"));
        assert!(refused(b"Infinity"));
        assert!(refused(b"\xA01"));
        assert!(refused(b"1\xA0"));
    }

    #[test]
    fn a_nul_byte_anywhere_is_trailing_data() {
        assert!(refused(b"1\x002"));
        assert!(refused(b"\x001"));
        assert!(refused(b"1\x00"));
        assert!(refused(b"\x00"));
    }

    #[test]
    fn negative_zero_keeps_the_engine_values() {
        assert_eq!(long(b"-0"), 0);
        assert_eq!(long(b"\x0B-0"), 0);
        assert!(double(b"-0.0").is_sign_negative());
        assert!(double(b"\x0C-0.0").is_sign_negative());
        assert!(double(b"-0e0").is_sign_negative());
    }
}
