//! PHP value construction through the engine's C date API.

use std::ffi::{c_char, c_int, c_uint, c_void};

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendObject, Zval};

use crate::exception::ext_exception;
use crate::php::{self, call_handle, ensure_no_pending_exception};

extern "C" {
    fn php_date_instantiate(pce: *mut ext_php_rs::zend::ClassEntry, object: *mut Zval) -> *mut Zval;

    #[cfg(php84)]
    fn php_date_initialize_from_ts_long(dateobj: *mut c_void, sec: i64, usec: c_int);

    fn php_date_initialize(
        dateobj: *mut c_void,
        time_str: *const c_char,
        time_str_len: usize,
        format: *const c_char,
        timezone_object: *mut Zval,
        flags: c_int,
    ) -> bool;
}

const PHP_DATE_OBJ_STD_OFFSET: usize = std::mem::size_of::<*const c_void>();

/// `php_date_obj_from_obj`: the `php_date_obj` a datetime `zend_object` is embedded in.
fn php_date_obj(object: &mut ZendObject) -> *mut c_void {
    std::ptr::from_mut(object)
        .cast::<u8>()
        .wrapping_sub(PHP_DATE_OBJ_STD_OFFSET)
        .cast::<c_void>()
}

// `timelib_time` up to `tim_uptodate` (ext/date/lib/timelib.h, identical in the 2022.14-2022.17 timelib of PHP 8.3-8.5)
#[repr(C)]
struct TimelibSpecial {
    kind: c_uint,
    amount: i64,
}

#[repr(C)]
struct TimelibRelTime {
    y: i64,
    m: i64,
    d: i64,
    h: i64,
    i: i64,
    s: i64,
    us: i64,
    weekday: c_int,
    weekday_behavior: c_int,
    first_last_day_of: c_int,
    invert: c_int,
    days: i64,
    special: TimelibSpecial,
    have_weekday_relative: c_uint,
    have_special_relative: c_uint,
}

#[repr(C)]
struct TimelibTime {
    y: i64,
    m: i64,
    d: i64,
    h: i64,
    i: i64,
    s: i64,
    us: i64,
    z: c_int,
    tz_abbr: *mut c_char,
    tz_info: *mut c_void,
    dst: c_int,
    relative: TimelibRelTime,
    sse: i64,
    have_time: c_uint,
    have_date: c_uint,
    have_zone: c_uint,
    have_relative: c_uint,
    have_weeknr_day: c_uint,
    sse_uptodate: c_uint,
    tim_uptodate: c_uint,
}

/// The up-to-date `timelib_time` of a `DateTimeImmutable` (or subclass) object.
fn timelib_time(datetime: &ZendObject) -> Option<&TimelibTime> {
    let dateobj = std::ptr::from_ref(datetime)
        .cast::<u8>()
        .wrapping_sub(PHP_DATE_OBJ_STD_OFFSET)
        .cast::<*const TimelibTime>();
    let time = unsafe { (*dateobj).as_ref() }?;

    (time.sse_uptodate != 0 && time.tim_uptodate != 0).then_some(time)
}

/// `getTimestamp() * 1_000_000 + format('u')`, `None` on i64 overflow.
pub fn datetime_micros(datetime: &ZendObject) -> Option<i64> {
    let time = timelib_time(datetime)?;

    time.sse.checked_mul(1_000_000)?.checked_add(time.us)
}

/// Days since the epoch of the value's own calendar day, `None` outside i32.
pub fn datetime_days(datetime: &ZendObject) -> Option<i64> {
    let time = timelib_time(datetime)?;
    let days = days_from_civil(time.y, time.m, time.d)?;

    i32::try_from(days).is_ok().then_some(days)
}

/// Days since 1970-01-01 of a proleptic Gregorian date, `None` on i64 overflow.
fn days_from_civil(year: i64, month: i64, day: i64) -> Option<i64> {
    let year = year - i64::from(month <= 2);
    let era = (if year >= 0 { year } else { year.checked_sub(399)? }) / 400;
    let year_of_era = year - era * 400;
    let day_of_year = (153 * (if month > 2 { month - 3 } else { month + 9 }) + 2) / 5 + day - 1;
    let day_of_era = year_of_era * 365 + year_of_era / 4 - year_of_era / 100 + day_of_year;

    era.checked_mul(146_097)?.checked_add(day_of_era - 719_468)
}

/// A fresh `DateTimeImmutable` initialised by `init` over its `php_date_obj`, then moved into `zone`.
fn datetime_in(
    zone: &[u8],
    init: impl FnOnce(*mut c_void) -> bool,
    failure: impl FnOnce() -> PhpException,
) -> Result<Zval, PhpException> {
    let fns = php::datetime_immutable()?;
    let mut datetime = Zval::new();

    unsafe {
        php_date_instantiate(std::ptr::from_ref(fns.ce).cast_mut(), std::ptr::from_mut(&mut datetime));
    }

    let datetime_obj = datetime
        .object_mut()
        .ok_or_else(|| ext_exception("arrow failed to instantiate a datetime object"))?;
    let initialized = init(php_date_obj(datetime_obj));

    ensure_no_pending_exception("restore a datetime value")?;

    if !initialized {
        return Err(failure());
    }

    let datetime = call_handle(
        fns.set_timezone,
        Some(datetime_obj),
        &mut [php::timezone(zone)?],
        "restore a datetime timezone",
    )?;

    if !datetime.is_object() {
        return Err(ext_exception("arrow expected setTimezone to return a datetime object"));
    }

    Ok(datetime)
}

/// The instant `micros` in `zone`.
pub fn datetime_from_micros(micros: i64, zone: &[u8]) -> Result<Zval, PhpException> {
    let seconds = micros.div_euclid(1_000_000);
    let fraction = micros.rem_euclid(1_000_000);

    datetime_in(
        zone,
        |dateobj| {
            // the createFromTimestamp() initializer, exported from PHP 8.4
            #[cfg(php84)]
            {
                unsafe { php_date_initialize_from_ts_long(dateobj, seconds, fraction as c_int) };

                true
            }

            #[cfg(not(php84))]
            {
                // timelib reads the byte AFTER the consumed input, so the buffer must be NUL-terminated like the
                // zend_strings PHP hands it (length excludes the NUL)
                let mut time_str = format!("{seconds}.{fraction:06}\0");

                unsafe {
                    php_date_initialize(
                        dateobj,
                        time_str.as_mut_ptr().cast::<c_char>(),
                        time_str.len() - 1,
                        c"U.u".as_ptr(),
                        std::ptr::null_mut(),
                        // PHP_DATE_INIT_FORMAT - the flags createFromFormat passes
                        0x02,
                    )
                }
            }
        },
        || {
            ext_exception(format!(
                "arrow failed to restore datetime from \"{micros}\" microseconds"
            ))
        },
    )
}

/// `new DateTimeImmutable('@' . ($days * 86_400))` in UTC.
pub fn date_from_days(days: i64) -> Result<Zval, PhpException> {
    let mut time_str = format!("@{}\0", days * 86_400);

    datetime_in(
        b"UTC",
        |dateobj| unsafe {
            php_date_initialize(
                dateobj,
                time_str.as_mut_ptr().cast::<c_char>(),
                time_str.len() - 1,
                std::ptr::null(),
                std::ptr::null_mut(),
                0,
            )
        },
        || ext_exception(format!("arrow failed to restore date from \"{days}\" days")),
    )
}

/// The lowercase 36-char form of 16 uuid bytes.
pub fn uuid_text(bytes: &[u8]) -> Vec<u8> {
    const HEX: &[u8; 16] = b"0123456789abcdef";
    let mut text = Vec::with_capacity(36);

    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push(b'-');
        }

        text.push(HEX[usize::from(byte >> 4)]);
        text.push(HEX[usize::from(byte & 0x0f)]);
    }

    text
}

/// `hex2bin()` of a 36-char uuid with its dashes removed.
pub fn uuid_bytes(text: &[u8]) -> Option<[u8; 16]> {
    let mut bytes = [0u8; 16];
    let mut digits = text
        .iter()
        .filter(|byte| **byte != b'-')
        .map(|byte| (*byte as char).to_digit(16));

    for byte in &mut bytes {
        let high = digits.next()??;
        let low = digits.next()??;
        *byte = (high * 16 + low) as u8;
    }

    digits.next().is_none().then_some(bytes)
}
