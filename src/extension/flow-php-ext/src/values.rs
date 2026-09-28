//! PHP value construction through the engine's C date API.

use std::ffi::{c_char, c_int, c_uint, c_void};

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendObject, ZendStr, Zval};
use ext_php_rs::zend::ExecutorGlobals;

use crate::ctx::{self, call_handle, ensure_no_pending_exception, write_property_raw, write_slot, zval_long, zval_str};
use crate::date_check::days_from_civil;
use crate::exception::ext_exception;

extern "C" {
    fn php_date_instantiate(pce: *mut ext_php_rs::zend::ClassEntry, object: *mut Zval)
        -> *mut Zval;

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

/// `DateTimePhysical::toPhysical()`: `getTimestamp() * 1_000_000 + format('u')`, `None` on i64 overflow.
pub fn datetime_micros(datetime: &ZendObject) -> Option<i64> {
    let time = timelib_time(datetime)?;

    time.sse.checked_mul(1_000_000)?.checked_add(time.us)
}

/// `DatePhysical::toPhysical()`: days since the epoch of the value's own calendar day, `None` outside i32.
pub fn datetime_days(datetime: &ZendObject) -> Option<i64> {
    let time = timelib_time(datetime)?;
    let days = days_from_civil(time.y, time.m, time.d)?;

    i32::try_from(days).is_ok().then_some(days)
}

/// A fresh `DateTimeImmutable` initialised by `init` over its `php_date_obj`, then moved into `zone`.
fn datetime_in(zone: &[u8], init: impl FnOnce(*mut c_void) -> bool, failure: impl FnOnce() -> PhpException) -> Result<Zval, PhpException> {
    let fns = ctx::datetime_immutable()?;
    let mut datetime = Zval::new();

    unsafe {
        php_date_instantiate(std::ptr::from_ref(fns.ce).cast_mut(), std::ptr::from_mut(&mut datetime));
    }

    let datetime_obj = datetime
        .object_mut()
        .ok_or_else(|| ext_exception("flow_php failed to instantiate a datetime object"))?;
    let initialized = init(php_date_obj(datetime_obj));
    ensure_no_pending_exception("restore a datetime value")?;

    if !initialized {
        return Err(failure());
    }

    let datetime = call_handle(fns.set_timezone, Some(datetime_obj), &mut [ctx::timezone(zone)?], "restore a datetime timezone")?;

    if !datetime.is_object() {
        return Err(ext_exception("flow_php expected setTimezone to return a datetime object"));
    }

    Ok(datetime)
}

/// `DateTimePhysical::fromPhysical()`: the instant `micros` in `zone`.
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
        || ext_exception(format!("flow_php failed to restore datetime from \"{micros}\" microseconds")),
    )
}

/// `DatePhysical::fromPhysical()`: `new DateTimeImmutable('@' . ($days * 86_400))` in UTC.
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
        || ext_exception(format!("flow_php failed to restore date from \"{days}\" days")),
    )
}

/// `TimePhysical::fromPhysical()`: `PT%dH%dM%dS`, then `f` and `invert`; `days` stays `false`.
pub fn interval_from_micros(micros: i64) -> Result<Zval, PhpException> {
    let magnitude = micros.unsigned_abs();
    let spec = format!(
        "PT{}H{}M{}S",
        magnitude / 3_600_000_000,
        magnitude / 60_000_000 % 60,
        magnitude / 1_000_000 % 60
    );
    let fns = ctx::interval()?;
    let mut interval = ZendObject::new(fns.ce);
    call_handle(fns.construct, Some(&mut interval), &mut [zval_str(spec.as_bytes())], "construct DateInterval")?;

    let mut fraction = Zval::new();
    fraction.set_double((magnitude % 1_000_000) as f64 / 1_000_000.0);
    write_property_raw(&mut interval, &mut ZendStr::new("f", false), fraction)?;
    write_property_raw(&mut interval, &mut ZendStr::new("invert", false), zval_long(i64::from(micros < 0)))?;

    let mut zv = Zval::new();
    zv.set_object(&mut interval);

    Ok(zv)
}

/// `Uuid::fromBytes()`: the constructor-less instance holding the lowercase 36-char form.
pub fn uuid_from_bytes(bytes: &[u8]) -> Result<Zval, PhpException> {
    const HEX: &[u8; 16] = b"0123456789abcdef";

    let mut text = Vec::with_capacity(36);

    for (index, byte) in bytes.iter().enumerate() {
        if matches!(index, 4 | 6 | 8 | 10) {
            text.push(b'-');
        }

        text.push(HEX[usize::from(byte >> 4)]);
        text.push(HEX[usize::from(byte & 0x0f)]);
    }

    let (uuid_ce, value_slot) = ctx::uuid()?;
    let mut uuid = ZendObject::new(uuid_ce);
    write_slot(&mut uuid, value_slot, zval_str(&text));

    let mut zv = Zval::new();
    zv.set_object(&mut uuid);

    Ok(zv)
}

/// `JsonFactory::create()`: the constructor-less instance, `isObject` read from the first byte.
pub fn json_from_bytes(bytes: &[u8]) -> Result<Zval, PhpException> {
    let (json_ce, value_slot, is_object_slot) = ctx::json()?;
    let mut json = ZendObject::new(json_ce);
    write_slot(&mut json, value_slot, zval_str(bytes));

    let mut is_object = Zval::new();
    is_object.set_bool(bytes.first() == Some(&b'{'));
    write_slot(&mut json, is_object_slot, is_object);

    let mut zv = Zval::new();
    zv.set_object(&mut json);

    Ok(zv)
}

/// `hex2bin()` of a 36-char uuid with its dashes removed.
pub fn uuid_bytes(text: &[u8]) -> Option<[u8; 16]> {
    let mut bytes = [0u8; 16];
    let mut digits = text.iter().filter(|byte| **byte != b'-').map(|byte| (*byte as char).to_digit(16));

    for byte in &mut bytes {
        let high = digits.next()??;
        let low = digits.next()??;
        *byte = (high * 16 + low) as u8;
    }

    digits.next().is_none().then_some(bytes)
}

/// `new DateTimeImmutable($str, $timezone)` through the same C-level timelib parser, in its
/// non-throwing `date_create()` flavor: `Ok(None)` on parse failure, no exception.
pub(crate) fn date_from_free_form(
    bytes: &[u8],
    timezone: Option<&mut Zval>,
) -> Result<Option<Zval>, PhpException> {
    let ce = ctx::datetime_immutable()?.ce;

    let mut datetime = Zval::new();
    unsafe {
        php_date_instantiate(
            std::ptr::from_ref(ce).cast_mut(),
            std::ptr::from_mut(&mut datetime),
        );
    }

    let datetime_obj = datetime
        .object_mut()
        .ok_or_else(|| ext_exception("flow_php failed to instantiate a datetime object"))?;

    // timelib reads the byte AFTER the consumed input, so the buffer must be
    // NUL-terminated like the zend_strings PHP hands it (length excludes the NUL)
    let mut time_str = Vec::with_capacity(bytes.len() + 1);
    time_str.extend_from_slice(bytes);
    time_str.push(0);

    let initialized = unsafe {
        php_date_initialize(
            php_date_obj(datetime_obj),
            time_str.as_mut_ptr().cast::<c_char>(),
            time_str.len() - 1,
            std::ptr::null(),
            timezone.map_or(std::ptr::null_mut(), std::ptr::from_mut),
            0,
        )
    };

    if !initialized {
        // date_create() reports parse failures via `false`; discard any engine
        // artifact so the caller can fall back to the PHP cast cleanly
        drop(ExecutorGlobals::take_exception());

        return Ok(None);
    }

    ensure_no_pending_exception("parse a datetime value")?;

    Ok(Some(datetime))
}
