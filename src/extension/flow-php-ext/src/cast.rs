use std::os::raw::{c_char, c_int};

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendObject, ZendStr, Zval};
use ext_php_rs::zend::{ClassEntry, Function};

use crate::ctx::{
    self, array_key_index, call_handle_on, call_handle_transparent, expect_object, null_zval, write_slot, zval_long,
};
use crate::date_check::{iso_date_gate, iso_date_time_gate, IsoSuffix};
use crate::exception::ext_exception;
use crate::json_check::json_valid;
use crate::uuid_check::is_uuid;
use crate::values::date_from_free_form;
use flow_batch_frame::kind::TypeJson;

extern "C" {
    fn zval_get_long_func(op: *const Zval, is_strict: bool) -> i64;
    fn zval_get_double_func(op: *const Zval) -> f64;
    fn zval_get_string_func(op: *mut Zval) -> *mut ZendStr;
    fn _is_numeric_string_ex(
        str: *const c_char,
        length: usize,
        lval: *mut i64,
        dval: *mut f64,
        allow_errors: bool,
        oflow_info: *mut c_int,
        trailing_data: *mut bool,
    ) -> u8;
}

pub enum MapKeyKind {
    Int,
    Str,
}

pub struct StructureElement {
    pub name: String,
    pub kind: CastKind,
    pub required: bool,
}

pub enum CastKind {
    Integer,
    PositiveInteger,
    Float,
    Boolean,
    String,
    NonEmptyString,
    DateTime(Vec<u8>),
    Date,
    Uuid,
    Json,
    List(Box<CastKind>),
    Map(MapKeyKind, Box<CastKind>),
    Structure(Vec<StructureElement>),
    Optional(Box<CastKind>),
    Fallback,
}

fn build_structure_kind(type_json: &TypeJson) -> CastKind {
    let mut elements = Vec::new();

    for field in type_json.fields() {
        // numeric element names would list-coerce the assembled array -
        // PHP's final assert has odd edge behavior there, not worth mirroring
        if array_key_index(field.name.as_bytes()).is_some() {
            return CastKind::Fallback;
        }

        elements.push(StructureElement {
            name: field.name.clone(),
            kind: build_cast_kind(&field.type_),
            required: !field.optional,
        });
    }

    if elements.is_empty() {
        return CastKind::Fallback;
    }

    CastKind::Structure(elements)
}

pub(crate) fn build_cast_kind(type_json: &TypeJson) -> CastKind {
    match type_json.type_.as_str() {
        "integer" => CastKind::Integer,
        "positive_integer" => CastKind::PositiveInteger,
        "float" => CastKind::Float,
        "boolean" => CastKind::Boolean,
        "string" => CastKind::String,
        "non_empty_string" => CastKind::NonEmptyString,
        "datetime" => match type_json.zone() {
            Some(zone) => CastKind::DateTime(zone.to_vec()),
            None => CastKind::Fallback,
        },
        "date" => CastKind::Date,
        "uuid" => CastKind::Uuid,
        "json" => CastKind::Json,
        "list" => match type_json.element() {
            Some(element) => CastKind::List(Box::new(build_cast_kind(element))),
            None => CastKind::Fallback,
        },
        "map" => match (type_json.key(), type_json.value()) {
            (Some(key), Some(value)) => match key.type_.as_str() {
                "integer" => CastKind::Map(MapKeyKind::Int, Box::new(build_cast_kind(value))),
                "string" => CastKind::Map(MapKeyKind::Str, Box::new(build_cast_kind(value))),
                _ => CastKind::Fallback,
            },
            _ => CastKind::Fallback,
        },
        "structure_v2" => build_structure_kind(type_json),
        "optional" => match type_json.base() {
            Some(base) => CastKind::Optional(Box::new(build_cast_kind(base))),
            None => CastKind::Fallback,
        },
        _ => CastKind::Fallback,
    }
}

fn engine_long(value: &Zval) -> i64 {
    unsafe { zval_get_long_func(std::ptr::from_ref(value), false) }
}

fn engine_double(value: &Zval) -> f64 {
    unsafe { zval_get_double_func(std::ptr::from_ref(value)) }
}

fn engine_string(value: &Zval) -> Result<ZBox<ZendStr>, PhpException> {
    let raw = unsafe { zval_get_string_func(std::ptr::from_ref(value).cast_mut()) };

    unsafe { raw.as_mut() }
        .map(|string| unsafe { ZBox::from_raw(string) })
        .ok_or_else(|| ext_exception("flow_php failed to convert a value to string"))
}

/// `mb_strtolower` + table match from `BooleanType::cast`. ASCII lowering
/// suffices: no non-ASCII uppercase code point lowercases onto these words.
pub(crate) fn bool_from_str(bytes: &[u8]) -> Option<bool> {
    if bytes.is_empty() || bytes.len() > 5 {
        return None;
    }

    let mut lowered = [0u8; 5];
    let lowered = &mut lowered[..bytes.len()];

    for (target, source) in lowered.iter_mut().zip(bytes) {
        *target = source.to_ascii_lowercase();
    }

    match &*lowered {
        b"true" | b"1" | b"yes" | b"on" => Some(true),
        b"false" | b"0" | b"no" | b"off" => Some(false),
        _ => None,
    }
}

/// The cheap prefix of `Json::isValid`: non-empty + matching `{}`/`[]` pair.
pub(crate) fn json_gate(bytes: &[u8]) -> bool {
    bytes.len() >= 2
        && ((bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}')
            || (bytes[0] == b'[' && bytes[bytes.len() - 1] == b']'))
}

/// `source` is a gated JSON string; the Json shares its zend_string instead of copying the bytes.
fn json_object_from(source: &Zval) -> Result<Zval, PhpException> {
    let bytes = source
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected a JSON string"))?
        .as_bytes();
    let is_object = bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}';

    let (json_ce, value_slot, is_object_slot) = ctx::json()?;
    let mut json = ZendObject::new(json_ce);
    write_slot(&mut json, value_slot, source.shallow_clone());

    let mut is_object_zv = Zval::new();
    is_object_zv.set_bool(is_object);
    write_slot(&mut json, is_object_slot, is_object_zv);

    let mut zv = Zval::new();
    zv.set_object(&mut json);

    Ok(zv)
}

/// `IS_LONG` / `IS_DOUBLE` from zend_types.h - the codes `_is_numeric_string_ex` returns.
const IS_LONG: u8 = 4;
const IS_DOUBLE: u8 = 5;

/// PHP's own `is_numeric()`, so the native path accepts exactly the strings
/// `IntegerType`/`FloatType::cast` accept and bails on the rest instead of
/// reproducing the grammar and drifting from it. Yields the type code together
/// with the value `_is_numeric_string_ex` already computed, so the integer cast
/// reads one parse instead of parsing the same bytes a second time.
///
/// The engine scans past `length` until a non-digit (it expects a zend_string's
/// NUL terminator), so a CSV cell - a slice of the record buffer - is copied
/// NUL-terminated first; unterminated, `"1"` followed by the next field's
/// `"2..."` reads as trailing data and is refused.
fn is_numeric_str(bytes: &[u8]) -> (u8, i64, f64) {
    let mut lval: i64 = 0;
    let mut dval: f64 = 0.0;
    let mut short = [0u8; 64];
    let long;
    let terminated: &[u8] = if bytes.len() < short.len() {
        short[..bytes.len()].copy_from_slice(bytes);
        &short
    } else {
        long = [bytes, &[0]].concat();
        &long
    };

    let code = unsafe {
        _is_numeric_string_ex(
            terminated.as_ptr().cast::<c_char>(),
            bytes.len(),
            &mut lval,
            &mut dval,
            false,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
        )
    };

    (code, lval, dval)
}

/// `IntegerType::cast`'s integer-shaped-text test. Text of this shape that
/// `_is_numeric_string_ex` had to widen to a double is text that does not fit
/// an i64, which PHP's round-trip branch refuses - the two spellings differ:
/// `'-9223372036854775809'` throws, `'-9.223372036854776e18'` casts.
fn integer_shaped(bytes: &[u8]) -> bool {
    let body = bytes
        .iter()
        .position(|b| !b.is_ascii_whitespace())
        .map_or(&[][..], |start| {
            let end = bytes.iter().rposition(|b| !b.is_ascii_whitespace()).unwrap_or(start);
            &bytes[start..=end]
        });

    let digits = match body.first() {
        Some(b'+' | b'-') => &body[1..],
        _ => body,
    };

    !digits.is_empty() && digits.iter().all(u8::is_ascii_digit)
}

/// `IntegerType::cast`'s range check: a double outside i64 yields `0` under a
/// plain cast rather than failing, so it is refused instead.
fn fits_i64(value: f64) -> bool {
    value.is_finite() && (-9223372036854775808.0..9223372036854775808.0).contains(&value)
}

/// A string the numeric casts can take: anything else is refused by the PHP
/// implementation, so the fast path hands it back rather than guessing.
fn numeric_scalar(value: &Zval) -> bool {
    match value.zend_str() {
        Some(string) => is_numeric_str(string.as_bytes()).0 != 0,
        None => value.is_bool(),
    }
}

/// `IntegerType::cast` of a string: `None` for any string it refuses or that needs its round-trip branch. A form feed is
/// whitespace to `is_numeric()` but not to `trim()`, so PHP's round-trip refuses it: it bails.
pub(crate) fn integer_from_str(bytes: &[u8]) -> Option<i64> {
    match is_numeric_str(bytes) {
        (IS_LONG, long, _) if !bytes.contains(&0x0C) => Some(long),
        (IS_DOUBLE, _, _) if integer_shaped(bytes) => None,
        (IS_DOUBLE, _, double) if fits_i64(double) => Some(double as i64),
        _ => None,
    }
}

/// `StringType::cast` of a non-finite float: the spelling the text writers produce, built without PHP's `(string)`,
/// which warns on NAN.
fn non_finite_text(value: f64) -> Option<&'static [u8]> {
    match value {
        value if value.is_nan() => Some(b"NAN"),
        f64::INFINITY => Some(b"INF"),
        f64::NEG_INFINITY => Some(b"-INF"),
        _ => None,
    }
}

/// The three non-finite spellings the text writers produce and `FloatType::cast` reads back, case-sensitive.
fn named_float(bytes: &[u8]) -> Option<f64> {
    match bytes {
        b"NAN" => Some(f64::NAN),
        b"INF" => Some(f64::INFINITY),
        b"-INF" => Some(f64::NEG_INFINITY),
        _ => None,
    }
}

/// `FloatType::cast` of a numeric string: the engine's `(float)` of it, which `is_numeric()` already computed; an integer
/// zero keeps the sign of its text, as `(float) '-0'` is `-0.0`.
pub(crate) fn float_from_str(bytes: &[u8]) -> Option<f64> {
    if let Some(named) = named_float(bytes) {
        return Some(named);
    }

    match is_numeric_str(bytes) {
        (IS_LONG, 0, _) if bytes.iter().find(|byte| !byte.is_ascii_whitespace() && **byte != 0x0B) == Some(&b'-') => {
            Some(-0.0)
        }
        (IS_LONG, long, _) => Some(long as f64),
        (IS_DOUBLE, _, double) => Some(double),
        _ => None,
    }
}

/// The instant an ISO string names, parsed as `DateTimeType::cast` parses it (before moving it into the column zone);
/// any other string is gated on StringTemporalParts, a PHP class, so it bails.
pub(crate) fn parse_iso_datetime(bytes: &[u8], zone: &[u8]) -> Result<Option<(IsoSuffix, Zval)>, PhpException> {
    Ok(
        match iso_date_time_gate(bytes).or_else(|| iso_date_gate(bytes).then_some(IsoSuffix::Naive)) {
            Some(IsoSuffix::Zulu) => {
                // mirrors DateTimeType::cast's Z branch
                let local = bytes.strip_suffix(b"\n").unwrap_or(bytes);
                let local = local.strip_suffix(b"Z").unwrap_or(local);
                let mut utc = ctx::timezone(b"UTC")?;

                date_from_free_form(local, Some(&mut utc))?.map(|instant| (IsoSuffix::Zulu, instant))
            }
            Some(IsoSuffix::Offset) => date_from_free_form(bytes, None)?.map(|instant| (IsoSuffix::Offset, instant)),
            Some(IsoSuffix::Naive) => {
                let mut column = ctx::timezone(zone)?;

                date_from_free_form(bytes, Some(&mut column))?.map(|instant| (IsoSuffix::Naive, instant))
            }
            None => None,
        },
    )
}

/// `DateType::cast` of an ISO date string: midnight UTC.
pub(crate) fn parse_iso_date(bytes: &[u8]) -> Result<Option<Zval>, PhpException> {
    if !iso_date_gate(bytes) {
        return Ok(None);
    }

    let mut utc = ctx::timezone(b"UTC")?;

    date_from_free_form(bytes, Some(&mut utc))
}

/// Casts one leaf value natively (containers: `builder.rs` `append_cast`); `Ok(None)` bails the WHOLE column value to the
/// PHP fallback (`Type::cast`), which reproduces results, exception classes,
/// messages and `previous` chains by re-running the canonical implementation.
/// PHP calls made inside a branch discard their own thrown exceptions and bail
/// instead - mirroring the `catch (Throwable)` wrappers in the PHP casts.
pub(crate) fn cast_value(kind: &CastKind, value: &Zval) -> Result<Option<Zval>, PhpException> {
    // Type::cast receives its argument by value; a branch that returns `value` itself must not hand
    // back the caller's reference
    let value = value.dereference();

    Ok(match kind {
        CastKind::Integer => {
            if value.is_long() {
                Some(value.shallow_clone())
            } else if value.is_double() {
                let double = engine_double(value);

                if fits_i64(double) {
                    Some(zval_long(double as i64))
                } else {
                    None
                }
            } else if let Some(string) = value.zend_str() {
                integer_from_str(string.as_bytes()).map(zval_long)
            } else if value.is_bool() {
                Some(zval_long(engine_long(value)))
            } else {
                None
            }
        }
        CastKind::PositiveInteger => match value.long() {
            Some(long) if long > 0 => Some(value.shallow_clone()),
            _ => None,
        },
        CastKind::Float => {
            if value.is_double() {
                Some(value.shallow_clone())
            } else if let Some(named) = value.zend_str().and_then(|string| named_float(string.as_bytes())) {
                let mut zv = Zval::new();
                zv.set_double(named);
                Some(zv)
            } else if value.is_long() || numeric_scalar(value) {
                let mut zv = Zval::new();
                zv.set_double(engine_double(value));
                Some(zv)
            } else {
                None
            }
        }
        CastKind::Boolean => {
            if value.is_bool() {
                Some(value.shallow_clone())
            } else if let Some(string) = value.zend_str() {
                // an unrecognised word is refused by BooleanType::cast, so bail instead of coercing
                bool_from_str(string.as_bytes()).map(|parsed| {
                    let mut zv = Zval::new();
                    zv.set_bool(parsed);
                    zv
                })
            } else if value.double().is_some_and(|double| !double.is_finite()) {
                None
            } else if value.is_long() || value.is_double() || value.is_null() {
                let mut zv = Zval::new();
                zv.set_bool(value.coerce_to_bool());
                Some(zv)
            } else {
                None
            }
        }
        CastKind::String => {
            if value.is_string() {
                Some(value.shallow_clone())
            } else if let Some(text) = value.double().and_then(non_finite_text) {
                Some(ctx::zval_str(text))
            } else if value.is_long() || value.is_double() {
                let mut zv = Zval::new();
                zv.set_zend_string(engine_string(value)?);
                Some(zv)
            } else if value.is_true() || value.is_false() {
                Some(ctx::bool_str(value.is_true())?)
            } else {
                None
            }
        }
        CastKind::NonEmptyString => {
            if value.zend_str().is_some_and(|s| !s.as_bytes().is_empty()) {
                Some(value.shallow_clone())
            } else if let Some(text) = value.double().and_then(non_finite_text) {
                Some(ctx::zval_str(text))
            } else if value.is_long() || value.is_double() {
                // engine string form of a number is never empty
                let mut zv = Zval::new();
                zv.set_zend_string(engine_string(value)?);
                Some(zv)
            } else if value.is_true() || value.is_false() {
                Some(ctx::bool_str(value.is_true())?)
            } else {
                None
            }
        }
        CastKind::DateTime(zone) => {
            if let Some(object) = value.object() {
                let immutable_ce = ctx::datetime_immutable()?.ce;

                if !object.instance_of(immutable_ce) {
                    None
                } else if zone_name(object)? == zone.as_slice() {
                    Some(value.shallow_clone())
                } else {
                    Some(in_zone(value, zone)?)
                }
            } else if let Some(string) = value.zend_str() {
                match parse_iso_datetime(string.as_bytes(), zone)? {
                    Some((IsoSuffix::Zulu, instant)) if zone.as_slice() != b"UTC" => Some(in_zone(&instant, zone)?),
                    Some((IsoSuffix::Offset, instant)) => Some(in_zone(&instant, zone)?),
                    Some((_, instant)) => Some(instant),
                    None => None,
                }
            } else if value.is_long() || value.double().is_some_and(f64::is_finite) {
                date_from_free_form(&timestamp_string(value)?, None)?
                    .map(|instant| in_zone(&instant, zone))
                    .transpose()?
            } else {
                None
            }
        }
        CastKind::Date => cast_date(value)?,
        CastKind::Uuid => {
            if let Some(object) = value.object() {
                let (uuid_ce, _) = ctx::uuid()?;

                if object.instance_of(uuid_ce) {
                    Some(value.shallow_clone())
                } else {
                    None
                }
            } else if let Some(string) = value.zend_str() {
                if is_uuid(string.as_bytes()) {
                    let (uuid_ce, value_slot) = ctx::uuid()?;
                    let mut uuid = ZendObject::new(uuid_ce);
                    write_slot(&mut uuid, value_slot, value.shallow_clone());

                    let mut zv = Zval::new();
                    zv.set_object(&mut uuid);
                    Some(zv)
                } else {
                    None
                }
            } else {
                None
            }
        }
        CastKind::Json => cast_json(value)?,
        CastKind::Optional(inner) => {
            if value.is_null() {
                Some(null_zval())
            } else {
                cast_value(inner, value)?
            }
        }
        CastKind::List(_) | CastKind::Map(..) | CastKind::Structure(_) | CastKind::Fallback => None,
    })
}

fn object_ce(object: &ZendObject) -> Result<&'static ClassEntry, PhpException> {
    unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("flow_php failed to resolve an object class"))
}

/// `"@" . $value` with the engine's number-to-string conversion.
fn timestamp_string(value: &Zval) -> Result<Vec<u8>, PhpException> {
    let number = engine_string(value)?;
    let mut bytes = Vec::with_capacity(number.as_bytes().len() + 1);
    bytes.push(b'@');
    bytes.extend_from_slice(number.as_bytes());

    Ok(bytes)
}

/// `$datetime->getTimezone()->getName()` of a DateTimeImmutable.
fn zone_name(datetime: &ZendObject) -> Result<Vec<u8>, PhpException> {
    let immutable_ce = ctx::datetime_immutable()?.ce;
    let get_timezone = ctx::datetime_get_timezone(immutable_ce)?;
    let timezone_zv = call_handle_on(get_timezone, datetime, &mut [], "read a timezone")?;
    let timezone = expect_object(&timezone_zv, "a timezone")?;
    let name = call_handle_on(ctx::timezone_get_name()?, timezone, &mut [], "read a timezone name")?;

    Ok(name
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected a timezone name string"))?
        .as_bytes()
        .to_vec())
}

/// `$datetime->setTimezone(new DateTimeZone($zone))` of a DateTimeImmutable.
fn in_zone(datetime: &Zval, zone: &[u8]) -> Result<Zval, PhpException> {
    let set_timezone = ctx::datetime_immutable()?.set_timezone;
    let timezone = ctx::timezone(zone)?;

    call_handle_on(
        set_timezone,
        expect_object(datetime, "a datetime value")?,
        &mut [timezone],
        "move a datetime into its column zone",
    )
}

fn cast_date(value: &Zval) -> Result<Option<Zval>, PhpException> {
    if let Some(object) = value.object() {
        let immutable_ce = ctx::datetime_immutable()?.ce;

        if !object.instance_of(immutable_ce) {
            return Ok(None);
        }

        let fns = ctx::datetime_cast_fns(object_ce(object)?)?;
        let (set_time, format) = (fns.set_time, fns.format);
        let his = ctx::string(b"H:i:s.u")?;

        // isValid passthrough: midnight keeps the SAME object (serialize
        // back-reference topology); a thrown format() bails like PHP's catch
        let Ok(formatted) = call_handle_transparent(format, Some(object), &mut [his]) else {
            return Ok(None);
        };

        if formatted.zend_str().map(ZendStr::as_bytes) == Some(b"00:00:00.000000") {
            return Ok(Some(value.shallow_clone()));
        }

        return set_midnight(set_time, object);
    }

    if let Some(string) = value.zend_str() {
        return parse_iso_date(string.as_bytes());
    }

    let parsed = if value.is_long() || value.double().is_some_and(f64::is_finite) {
        date_from_free_form(&timestamp_string(value)?, None)?
    } else {
        None
    };

    let Some(instant) = parsed else {
        return Ok(None);
    };
    let datetime = in_zone(&instant, b"UTC")?;
    let object = datetime
        .object()
        .ok_or_else(|| ext_exception("flow_php expected a datetime object"))?;
    let set_time = ctx::datetime_cast_fns(object_ce(object)?)?.set_time;

    set_midnight(set_time, object)
}

fn set_midnight(set_time: &'static Function, object: &ZendObject) -> Result<Option<Zval>, PhpException> {
    let mut args = [zval_long(0), zval_long(0), zval_long(0), zval_long(0)];

    let Ok(result) = call_handle_transparent(set_time, Some(object), &mut args) else {
        return Ok(None);
    };

    if !result.is_object() {
        return Ok(None);
    }

    Ok(Some(result))
}

fn cast_json(value: &Zval) -> Result<Option<Zval>, PhpException> {
    if let Some(object) = value.object() {
        let (json_ce, _, _) = ctx::json()?;

        if object.instance_of(json_ce) {
            return Ok(Some(value.shallow_clone()));
        }

        return Ok(None);
    }

    if let Some(string) = value.zend_str() {
        if !json_gate(string.as_bytes()) {
            return Ok(None);
        }

        if !json_valid(string.as_bytes()) {
            // a reject is never authoritative: the retained PHP Type::cast re-asks json_validate()
            return Ok(None);
        }

        return Ok(Some(json_object_from(value)?));
    }

    if value.is_array() {
        // Json::fromArray's json_encode, in its non-throwing flavor: `false`
        // (or a thrown JsonSerializable) bails to the PHP cast
        let Ok(encoded) = call_handle_transparent(ctx::json_encode()?, None, &mut [value.shallow_clone()]) else {
            return Ok(None);
        };

        let Some(string) = encoded.zend_str() else {
            return Ok(None);
        };

        if !json_gate(string.as_bytes()) {
            return Ok(None);
        }

        return Ok(Some(json_object_from(&encoded)?));
    }

    Ok(None)
}
