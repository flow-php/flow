//! Float text: the shortest digits that read back as the same float (`zend_gcvt()` in its `serialize_precision = -1`
//! mode, what `json_encode()` writes), whatever the `precision` and `serialize_precision` inis hold.

use std::ffi::{c_char, c_int, CStr};

use crate::text::Error;

extern "C" {
    fn zend_gcvt(value: f64, ndigit: c_int, dec_point: c_char, exponent: c_char, buf: *mut c_char) -> *mut c_char;
}

fn digits(out: &mut Vec<u8>, value: f64, zero_fraction: bool) {
    let mut buffer = [0 as c_char; 64];
    let text = unsafe {
        zend_gcvt(value, -1, b'.' as c_char, b'e' as c_char, buffer.as_mut_ptr());
        CStr::from_ptr(buffer.as_ptr())
    }
    .to_bytes();

    out.extend_from_slice(text);

    if zero_fraction && !text.contains(&b'.') {
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
