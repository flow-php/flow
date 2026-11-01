//! Float text as `json_encode()` writes it: the shortest round-trip digits, laid out as `zend_gcvt()` does.

use std::io::Write;

use crate::{int, Error};

/// `zend_gcvt()`'s `ndigit` for mode 0: a decimal point this many digits in or further right switches to E-style.
const FIXED_DIGITS: i32 = 17;

/// `{:e}` of a finite f64 is at most 24 bytes (`-2.2250738585072014e-308`).
const SCIENTIFIC: usize = 32;

/// `zend_gcvt(value, -1, '.', 'e')` of a finite value.
fn gcvt(out: &mut Vec<u8>, value: f64) {
    let mut scientific = [0u8; SCIENTIFIC];
    let mut cursor = std::io::Cursor::new(&mut scientific[..]);
    write!(cursor, "{value:e}").expect("the scientific form of a finite f64 fits");
    let len = cursor.position() as usize;
    let text = &scientific[..len];

    let (negative, text) = match text {
        [b'-', rest @ ..] => (true, rest),
        _ => (false, text),
    };
    let e = text
        .iter()
        .position(|byte| *byte == b'e')
        .expect("{:e} writes an exponent");
    let exponent = std::str::from_utf8(&text[e + 1..])
        .ok()
        .and_then(|exponent| exponent.parse::<i32>().ok())
        .expect("{:e} writes a decimal exponent");

    // the significant digits, `d.ddd` without its point
    let mut digits = [0u8; FIXED_DIGITS as usize];
    let mut count = 0;

    for byte in &text[..e] {
        if *byte != b'.' {
            digits[count] = *byte;
            count += 1;
        }
    }

    // `{:e}` takes the upper of two equally near candidates, `zend_dtoa()` the one with an even last digit
    if digits[count - 1] & 1 == 1 && halfway(value, count) {
        digits[count - 1] -= 1;
    }

    let digits = &digits[..count];
    let point = exponent + 1;

    if negative {
        out.push(b'-');
    }

    if !(-3..=FIXED_DIGITS).contains(&point) {
        // E-style: d.ddde±x, the fraction at least one digit, the exponent without padding
        out.push(digits[0]);
        out.push(b'.');

        if digits.len() == 1 {
            out.push(b'0');
        } else {
            out.extend_from_slice(&digits[1..]);
        }

        out.push(b'e');
        out.push(if point - 1 < 0 { b'-' } else { b'+' });
        int(out, i64::from((point - 1).abs()));
    } else if point < 0 {
        out.extend_from_slice(b"0.");
        out.resize(out.len() + point.unsigned_abs() as usize, b'0');
        out.extend_from_slice(digits);
    } else {
        let point = point as usize;

        for i in 0..point {
            out.push(*digits.get(i).unwrap_or(&b'0'));
        }

        if digits.len() > point {
            if point == 0 {
                out.push(b'0');
            }

            out.push(b'.');
            out.extend_from_slice(&digits[point..]);
        }
    }
}

/// Whether `value` is exactly halfway between its two nearest `digits`-digit decimals.
fn halfway(value: f64, digits: usize) -> bool {
    let bits = value.to_bits();
    let biased = (bits >> 52 & 0x7ff) as i32;
    let fraction = bits & ((1 << 52) - 1);
    let (mantissa, exponent) = if biased == 0 {
        (fraction, -1074)
    } else {
        (fraction | 1 << 52, biased - 1075)
    };

    if mantissa == 0 {
        return false;
    }

    let shift = mantissa.trailing_zeros();
    let exponent = exponent + shift as i32;

    if !(-25..0).contains(&exponent) {
        return false;
    }

    let expansion = u128::from(mantissa >> shift) * 5u128.pow(exponent.unsigned_abs());

    (10u128.pow(digits as u32)..10u128.pow(digits as u32 + 1)).contains(&expansion)
}

fn digits(out: &mut Vec<u8>, value: f64, zero_fraction: bool) {
    let start = out.len();

    gcvt(out, value);

    if zero_fraction && !out[start..].contains(&b'.') {
        out.extend_from_slice(b".0");
    }
}

/// CSV, XML and Text float text: always a fraction or an exponent (`1.0`, `1.0e+25`); `NAN`, `INF`, `-INF` by name.
pub fn text(out: &mut Vec<u8>, value: f64) {
    if value.is_nan() {
        out.extend_from_slice(b"NAN");
    } else if value.is_infinite() {
        out.extend_from_slice(if value > 0.0 { b"INF" } else { b"-INF" });
    } else {
        digits(out, value, true);
    }
}

/// JSON float text: `.0` only under `JSON_PRESERVE_ZERO_FRACTION`; NAN and the infinities are refused.
pub fn json(out: &mut Vec<u8>, value: f64, zero_fraction: bool) -> Result<(), Error> {
    if !value.is_finite() {
        return Err(Error::NonFinite);
    }

    digits(out, value, zero_fraction);

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::{json, text};

    fn gcvt(value: f64) -> String {
        let mut out = Vec::new();

        json(&mut out, value, false).unwrap();

        String::from_utf8(out).unwrap()
    }

    fn csv(value: f64) -> String {
        let mut out = Vec::new();

        text(&mut out, value);

        String::from_utf8(out).unwrap()
    }

    /// `json_encode()` of each literal under `serialize_precision = -1`.
    #[test]
    fn the_point_moves_up_to_seventeen_digits_in_and_three_out() {
        assert_eq!(gcvt(0.0), "0");
        assert_eq!(gcvt(-0.0), "-0");
        assert_eq!(gcvt(0.1), "0.1");
        assert_eq!(gcvt(0.5), "0.5");
        assert_eq!(gcvt(1.0), "1");
        assert_eq!(gcvt(100.0), "100");
        assert_eq!(gcvt(123.456), "123.456");
        assert_eq!(gcvt(0.1 + 0.2), "0.30000000000000004");
        assert_eq!(gcvt(1.0 / 3.0), "0.3333333333333333");
        assert_eq!(gcvt(1e15), "1000000000000000");
        assert_eq!(gcvt(1e16), "10000000000000000");
        assert_eq!(gcvt(123456789012345.67), "123456789012345.67");
        assert_eq!(gcvt(0.001), "0.001");
        assert_eq!(gcvt(0.0001), "0.0001");
        assert_eq!(gcvt(0.00012), "0.00012");
    }

    #[test]
    fn e_style_past_the_fixed_range_with_an_unpadded_exponent() {
        assert_eq!(gcvt(1e17), "1.0e+17");
        assert_eq!(gcvt(1e21), "1.0e+21");
        assert_eq!(gcvt(1e25), "1.0e+25");
        assert_eq!(gcvt(1.5e100), "1.5e+100");
        assert_eq!(gcvt(1e-5), "1.0e-5");
        assert_eq!(gcvt(1e-7), "1.0e-7");
        assert_eq!(gcvt(1.25e-7), "1.25e-7");
        assert_eq!(gcvt(f64::MAX), "1.7976931348623157e+308");
        assert_eq!(gcvt(-f64::MAX), "-1.7976931348623157e+308");
        assert_eq!(gcvt(f64::MIN_POSITIVE), "2.2250738585072014e-308");
        assert_eq!(gcvt(5e-324), "5.0e-324");
        assert_eq!(gcvt(1.5e-323), "1.5e-323");
        assert_eq!(gcvt(f64::from_bits(0x000F_FFFF_FFFF_FFFF)), "2.225073858507201e-308");
    }

    /// Values exactly between two shortest candidates: `{:e}` rounds up, `zend_dtoa()` to the even digit.
    #[test]
    fn a_halfway_value_takes_the_even_candidate_as_zend_dtoa_does() {
        assert_eq!(
            format!("{:e}", f64::from_bits(0x431c2ce18953b1dd)),
            "1.9826616327814313e15"
        );
        assert_eq!(gcvt(f64::from_bits(0x431c2ce18953b1dd)), "1982661632781431.2");
        assert_eq!(gcvt(f64::from_bits(0x42ad3c429c10aea0)), "16072326383703.312");
        assert_eq!(gcvt(f64::from_bits(0xc2d27561733ff268)), "-81181421666249.62");
        assert_eq!(gcvt(f64::from_bits(0xc2dbf17696eaed08)), "-122895562681268.12");
        assert_eq!(gcvt(f64::from_bits(0x431c2ce18953b1dd) + 0.5), "1982661632781431.8");
        assert_eq!(gcvt(0.5), "0.5");
        assert_eq!(gcvt(2.5), "2.5");
        assert_eq!(gcvt(1.25), "1.25");
    }

    #[test]
    fn a_zero_fraction_is_added_to_a_whole_number_only() {
        let mut out = Vec::new();

        json(&mut out, 1.0, true).unwrap();
        json(&mut out, -0.0, true).unwrap();
        json(&mut out, 1e25, true).unwrap();
        json(&mut out, 1.5, true).unwrap();

        assert_eq!(String::from_utf8(out).unwrap(), "1.0-0.01.0e+251.5");
        assert_eq!(csv(1.0), "1.0");
        assert_eq!(csv(1e-5), "1.0e-5");
    }

    #[test]
    fn non_finite_values_are_named_in_csv_and_refused_in_json() {
        assert_eq!(csv(f64::NAN), "NAN");
        assert_eq!(csv(f64::INFINITY), "INF");
        assert_eq!(csv(f64::NEG_INFINITY), "-INF");

        for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut out = b"kept".to_vec();

            assert_eq!(json(&mut out, value, true), Err(crate::Error::NonFinite));
            assert_eq!(out, b"kept");
        }
    }
}
