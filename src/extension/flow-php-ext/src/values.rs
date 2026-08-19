//! Wire-value decoding mirroring `Flow\Floe\ValueDecoder`.

use std::ffi::{c_char, c_int, c_void};

use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::ExecutorGlobals;

use crate::ctx::{
    call_handle, ensure_no_pending_exception, ht_insert, ht_insert_index, write_property_raw,
    write_slot, zval_long, zval_str, Ctx,
};
use crate::exception::ext_exception;
use crate::format::{
    Reader, DATETIME_IMMUTABLE, DATETIME_MUTABLE, VALUE_ABSENT, VALUE_NULL, VALUE_PRESENT,
};
use crate::plan::{Decoder, MapKey};

extern "C" {
    fn php_date_instantiate(pce: *mut ext_php_rs::zend::ClassEntry, object: *mut Zval)
        -> *mut Zval;

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

/// `new DateTimeImmutable($str)` through the same C-level timelib parser, in its
/// non-throwing `date_create()` flavor: `Ok(None)` on parse failure, no exception.
pub(crate) fn date_from_free_form(
    bytes: &[u8],
    ctx: &mut Ctx,
) -> Result<Option<Zval>, PhpException> {
    let ce = ctx.datetime_fns(false)?.ce;

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
            std::ptr::from_mut(datetime_obj)
                .cast::<u8>()
                .sub(PHP_DATE_OBJ_STD_OFFSET)
                .cast::<c_void>(),
            time_str.as_mut_ptr().cast::<c_char>(),
            time_str.len() - 1,
            std::ptr::null(),
            std::ptr::null_mut(),
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

pub fn decode_value(
    decoder: &Decoder,
    reader: &mut Reader,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let mut zv = Zval::new();

    match decoder {
        Decoder::Integer => zv.set_long(reader.i64("integer value")?),
        Decoder::Float => zv.set_double(reader.f64("float value")?),
        Decoder::Boolean => zv.set_bool(reader.u8("boolean value")? == 0x01),
        Decoder::String => {
            let length = reader.u32("string value")? as usize;
            zv.set_zend_string(ZendStr::new(reader.bytes(length, "string value")?, false));
        }
        Decoder::Null => zv.set_null(),
        Decoder::DateTime => zv = decode_datetime(reader, ctx)?,
        Decoder::Interval => zv = decode_interval(reader, ctx)?,
        Decoder::Uuid => zv = decode_uuid(reader, ctx)?,
        Decoder::Json => zv = decode_json(reader, ctx)?,
        Decoder::TimeZone => {
            let length = reader.u32("timezone value")? as usize;
            zv = ctx
                .timezone(reader.bytes(length, "timezone value")?)?
                .shallow_clone();
        }
        Decoder::Enum => zv = decode_enum(reader, ctx)?,
        Decoder::Xml => zv = decode_via_static(reader, ctx, "xmlDocumentFromString")?,
        Decoder::XmlElement => zv = decode_via_static(reader, ctx, "xmlElementFromString")?,
        Decoder::Html => zv = decode_via_static(reader, ctx, "htmlDocumentFromString")?,
        Decoder::HtmlElement => zv = decode_via_static(reader, ctx, "htmlElementFromString")?,
        Decoder::List(element) => {
            let count = reader.u32("list value")?;
            let mut values = ZendHashTable::with_capacity(count);

            for _ in 0..count {
                let value = decode_value(element, reader, ctx)?;
                values.push(value).map_err(|e| {
                    ext_exception(format!("flow_php failed to collect list values: {e:?}"))
                })?;
            }

            zv.set_hashtable(values);
        }
        Decoder::Map(key, value) => zv = decode_map(key, value, reader, ctx)?,
        Decoder::Structure(elements) => zv = decode_structure(elements, reader, ctx)?,
        Decoder::Optional(base) => {
            if reader.u8("optional value")? == VALUE_NULL {
                zv.set_null();
            } else {
                zv = decode_value(base, reader, ctx)?;
            }
        }
    }

    Ok(zv)
}

fn decode_map(
    key: &MapKey,
    value: &Decoder,
    reader: &mut Reader,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let count = reader.u32("map value")?;
    let mut values = ZendHashTable::with_capacity(count);

    for _ in 0..count {
        match key {
            MapKey::Integer => {
                let map_key = reader.i64("map key")?;
                let map_value = decode_value(value, reader, ctx)?;
                ht_insert_index(&mut values, map_key, map_value);
            }
            MapKey::String => {
                let length = reader.u32("map key")? as usize;
                let map_key = reader.bytes(length, "map key")?;
                let map_value = decode_value(value, reader, ctx)?;
                ht_insert(&mut values, map_key, map_value);
            }
        }
    }

    let mut zv = Zval::new();
    zv.set_hashtable(values);

    Ok(zv)
}

fn decode_structure(
    elements: &[(Vec<u8>, Decoder)],
    reader: &mut Reader,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    let mut structure = ZendHashTable::with_capacity(elements.len() as u32);

    for (name, element) in elements {
        let flag = reader.u8("structure element flag")?;

        match flag {
            VALUE_PRESENT => {
                let value = decode_value(element, reader, ctx)?;
                ht_insert(&mut structure, name, value);
            }
            VALUE_NULL => ht_insert(&mut structure, name, Zval::new()),
            VALUE_ABSENT => {}
            other => {
                return Err(ext_exception(format!(
                    "flow_php found unknown structure element flag 0x{other:02X}"
                )));
            }
        }
    }

    let mut zv = Zval::new();
    zv.set_hashtable(structure);

    Ok(zv)
}

/// Mirrors `ValueDecoder::decodeDateTime`.
fn decode_datetime(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let class_flag = reader.u8("datetime value")?;

    let timestamp;
    let microseconds;
    let timezone_name;

    let fns = match class_flag {
        DATETIME_IMMUTABLE | DATETIME_MUTABLE => {
            timestamp = reader.i64("datetime value")?;
            microseconds = reader.u32("datetime value")?;
            let timezone_length = reader.u32("datetime value")? as usize;
            timezone_name = reader.bytes(timezone_length, "datetime value")?;

            ctx.datetime_fns(class_flag == DATETIME_MUTABLE)?
        }
        other => {
            return Err(ext_exception(format!(
                "flow_php found unknown datetime flag 0x{other:02X}"
            )));
        }
    };

    let ce = fns.ce;
    let set_timezone = fns.set_timezone;

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
    let mut time_str = format!("{timestamp}.{microseconds:06}\0");
    let initialized = unsafe {
        php_date_initialize(
            std::ptr::from_mut(datetime_obj)
                .cast::<u8>()
                .sub(PHP_DATE_OBJ_STD_OFFSET)
                .cast::<c_void>(),
            time_str.as_mut_ptr().cast::<c_char>(),
            time_str.len() - 1,
            c"U.u".as_ptr(),
            std::ptr::null_mut(),
            // PHP_DATE_INIT_FORMAT - the flags createFromFormat passes.
            0x02,
        )
    };
    ensure_no_pending_exception("restore a datetime value")?;

    if !initialized {
        return Err(ext_exception(format!(
            "flow_php failed to restore datetime from timestamp \"{timestamp}\""
        )));
    }

    let timezone = ctx.timezone(timezone_name)?.shallow_clone();
    let datetime = call_handle(
        &set_timezone,
        Some(datetime_obj),
        &mut [timezone],
        "restore a datetime timezone",
    )?;

    if !datetime.is_object() {
        return Err(ext_exception(
            "flow_php expected setTimezone to return a datetime object",
        ));
    }

    Ok(datetime)
}

/// Mirrors `ValueDecoder::decodeInterval`; `days` is intentionally not
/// preserved (same as PHP).
fn decode_interval(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let invert = i64::from(reader.u8("time value")?);
    let mut fields = [0i64; 7];

    for field in &mut fields[..6] {
        *field = i64::from(reader.u32("time value")?);
    }

    fields[6] = invert;
    let fraction = reader.f64("time value")?;

    let interval_fns = ctx.interval()?;
    let mut interval = ZendObject::new(interval_fns.ce);
    call_handle(
        &interval_fns.construct,
        Some(&mut interval),
        &mut [interval_fns.spec.shallow_clone()],
        "construct DateInterval",
    )?;

    for (index, value) in fields.iter().enumerate() {
        write_property_raw(
            &mut interval,
            &mut interval_fns.names[index],
            zval_long(*value),
        )?;
    }

    let mut fraction_zv = Zval::new();
    fraction_zv.set_double(fraction);
    write_property_raw(&mut interval, &mut interval_fns.names[7], fraction_zv)?;

    let mut zv = Zval::new();
    zv.set_object(&mut interval);

    Ok(zv)
}

/// Mirrors `ValueDecoder::$createUuid`: constructor-less instantiation - the
/// data was written by Flow and is trusted.
fn decode_uuid(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let length = reader.u32("uuid value")? as usize;
    let value = reader.bytes(length, "uuid value")?;
    let (uuid_ce, value_slot) = ctx.uuid()?;
    let mut uuid = ZendObject::new(uuid_ce);
    write_slot(&mut uuid, value_slot, zval_str(value));

    let mut zv = Zval::new();
    zv.set_object(&mut uuid);

    Ok(zv)
}

/// Mirrors `ValueDecoder::$createJson`: constructor-less instantiation.
fn decode_json(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let length = reader.u32("json value")? as usize;
    let value = reader.bytes(length, "json value")?;
    let is_object = reader.u8("json value")? == 0x01;

    let (json_ce, value_slot, is_object_slot) = ctx.json()?;
    let mut json = ZendObject::new(json_ce);
    write_slot(&mut json, value_slot, zval_str(value));

    let mut is_object_zv = Zval::new();
    is_object_zv.set_bool(is_object);
    write_slot(&mut json, is_object_slot, is_object_zv);

    let mut zv = Zval::new();
    zv.set_object(&mut json);

    Ok(zv)
}

fn decode_enum(reader: &mut Reader, ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let class_length = reader.u32("enum value")? as usize;
    let class = reader.bytes(class_length, "enum value")?.to_vec();
    let case_length = reader.u32("enum value")? as usize;
    let case = reader.bytes(case_length, "enum value")?.to_vec();

    ctx.enum_case(&class, &case)
}

/// Markup DOM reconstruction delegated to the pure-PHP `ValueDecoder` public
/// statics - the exact code its decoders run.
fn decode_via_static(
    reader: &mut Reader,
    ctx: &mut Ctx,
    method: &'static str,
) -> Result<Zval, PhpException> {
    let length = reader.u32("markup value")? as usize;
    let markup_zv = zval_str(reader.bytes(length, "markup value")?);

    let handle = ctx.value_decoder_static(method)?;
    let value = call_handle(handle, None, &mut [markup_zv], "restore a markup value")?;

    if !value.is_object() {
        return Err(ext_exception(format!(
            "flow_php expected ValueDecoder::{method} to return an object"
        )));
    }

    Ok(value)
}
