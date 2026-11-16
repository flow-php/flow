//! PHP value construction through the engine's C date API.

use std::cell::OnceCell;
use std::ffi::{c_char, c_int, c_uint, c_void};
use std::sync::OnceLock;

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

// one prototype per zone, cloned per value: a string parse and a setTimezone() call per value cost 0.5 µs each
#[derive(Default)]
pub struct Instants {
    offset_zero: OnceCell<Zval>,
    utc: OnceCell<Zval>,
}

impl Instants {
    pub fn timestamp(&self, micros: i64) -> Result<Zval, PhpException> {
        if !clones_verified()? {
            return datetime_from_micros(micros, b"+00:00");
        }

        // SAFETY: clones_verified() proved TimelibTime matches this PHP's timelib_time
        unsafe { datetime_at(prototype(&self.offset_zero, b"+00:00")?, micros) }
    }

    pub fn date(&self, days: i64) -> Result<Zval, PhpException> {
        if !clones_verified()? {
            return date_from_days(days);
        }

        let Some(micros) = days.checked_mul(86_400_000_000) else {
            return date_from_days(days);
        };

        // SAFETY: clones_verified() proved TimelibTime matches this PHP's timelib_time
        unsafe { datetime_at(prototype(&self.utc, b"UTC")?, micros) }
    }
}

/// Zend API numbers of PHP 8.3, 8.4 and 8.5, whose timelib (2022.14-2022.17) lays `timelib_time` out as `TimelibTime`.
const VERIFIED_LAYOUT_APIS: [u32; 3] = [20_230_831, 20_240_924, 20_250_925];

fn layout_verified_for(api: u32) -> bool {
    VERIFIED_LAYOUT_APIS.contains(&api)
}

static CLONES_VERIFIED: OnceLock<bool> = OnceLock::new();

// a clone's timelib_time is written only where TimelibTime matches it, any other PHP keeps the parse path
fn clones_verified() -> Result<bool, PhpException> {
    if let Some(verified) = CLONES_VERIFIED.get() {
        return Ok(*verified);
    }

    let verified = layout_verified_for(ext_php_rs::ffi::ZEND_MODULE_API_NO) && {
        let probe = datetime_from_micros(1_234_567_890_123_456, b"+00:00")?;

        probe.object().and_then(timelib_time).is_some_and(|time| {
            (time.y, time.m, time.d, time.h, time.i, time.s, time.us, time.sse)
                == (2009, 2, 13, 23, 31, 30, 123_456, 1_234_567_890)
        })
    };

    Ok(*CLONES_VERIFIED.get_or_init(|| verified))
}

fn prototype<'a>(cell: &'a OnceCell<Zval>, zone: &[u8]) -> Result<&'a ZendObject, PhpException> {
    if cell.get().is_none() {
        let _ = cell.set(datetime_from_micros(0, zone)?);
    }

    cell.get()
        .and_then(Zval::object)
        .ok_or_else(|| ext_exception("arrow expected a datetime prototype"))
}

// the prototype's zone has no offset, so the local fields written here are the UTC ones
/// # Safety
/// `TimelibTime` must match this PHP's `timelib_time` layout, which `clones_verified()` checks.
unsafe fn datetime_at(prototype: &ZendObject, micros: i64) -> Result<Zval, PhpException> {
    let seconds = micros.div_euclid(1_000_000);
    let days = seconds.div_euclid(86_400);
    let second_of_day = seconds.rem_euclid(86_400);
    let (year, month, day) = civil_from_days(days);
    // SAFETY: a live object's handlers pointer is valid for its lifetime
    let clone_obj = unsafe { (*prototype.handlers).clone_obj }
        .ok_or_else(|| ext_exception("arrow expected a datetime to be cloneable"))?;
    // SAFETY: clone_obj reads the prototype and returns a new object, it never writes the prototype
    let object = unsafe { clone_obj(std::ptr::from_ref(prototype).cast_mut()) };
    let mut datetime = Zval::new();

    // SAFETY: clone_obj returns a new object holding one reference, which the zval takes over
    unsafe {
        datetime.set_object(&mut *object);
        (*object).gc.refcount -= 1;
    }

    // SAFETY: the caller guarantees the layout; date_object_clone gave the clone its own timelib_time
    let time = unsafe {
        &mut **std::ptr::from_mut(&mut *object)
            .cast::<u8>()
            .wrapping_sub(PHP_DATE_OBJ_STD_OFFSET)
            .cast::<*mut TimelibTime>()
    };
    time.y = year;
    time.m = month;
    time.d = day;
    time.h = second_of_day / 3_600;
    time.i = second_of_day / 60 % 60;
    time.s = second_of_day % 60;
    time.us = micros.rem_euclid(1_000_000);
    time.sse = seconds;

    Ok(datetime)
}

fn civil_from_days(days: i64) -> (i64, i64, i64) {
    let days = days + 719_468;
    let era = days.div_euclid(146_097);
    let day_of_era = days.rem_euclid(146_097);
    let year_of_era = (day_of_era - day_of_era / 1_460 + day_of_era / 36_524 - day_of_era / 146_096) / 365;
    let day_of_year = day_of_era - (365 * year_of_era + year_of_era / 4 - year_of_era / 100);
    let month_index = (5 * day_of_year + 2) / 153;
    let day = day_of_year - (153 * month_index + 2) / 5 + 1;
    let month = if month_index < 10 {
        month_index + 3
    } else {
        month_index - 9
    };

    (year_of_era + era * 400 + i64::from(month <= 2), month, day)
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn only_the_verified_php_minors_write_into_a_cloned_timelib_time() {
        assert!(layout_verified_for(20_230_831));
        assert!(layout_verified_for(20_240_924));
        assert!(layout_verified_for(20_250_925));
        assert!(!layout_verified_for(20_220_829));
        assert!(!layout_verified_for(20_260_000));
    }

    #[test]
    fn days_round_trip_through_the_civil_calendar() {
        for days in [-719_528_i64, -1, 0, 1, 11_016, 2_932_896, -1_000_000, 1_000_000] {
            let (year, month, day) = civil_from_days(days);

            assert_eq!(days_from_civil(year, month, day), Some(days));
        }

        assert_eq!(civil_from_days(0), (1970, 1, 1));
        assert_eq!(civil_from_days(11_016), (2000, 2, 29));
    }
}
