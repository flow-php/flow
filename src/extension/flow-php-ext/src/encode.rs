//! Floe ROW frame-body encoder mirroring `Flow\Floe\RowEncoder::encode`
//! byte-for-byte; entry properties are read through cached slot offsets.

use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::zend_ulong;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::ClassEntry;

use crate::ctx::{call_handle, call_handle_on, property_offset, read_property, Ctx};
use crate::exception::ext_exception;
use crate::format::{
    write_u32, DATETIME_IMMUTABLE, DATETIME_MUTABLE, KEY_INTEGER, KEY_STRING, TAG_ARRAY,
    TAG_BOOLEAN, TAG_DATETIME, TAG_FLOAT, TAG_INTEGER, TAG_JSON, TAG_NULL, TAG_STRING, TAG_UUID,
    VALUE_ABSENT, VALUE_NULL, VALUE_NULL_FROM_NULL, VALUE_PRESENT,
};
use crate::plan::{entry_class_for_type, parse_schema_json, TypeJson};

enum EncodeMapKey {
    Integer,
    String,
    Dynamic,
}

/// Declared structure element key after PHP's array-key coercion, mirroring
/// `ValueEncoder::structureEncoder`.
enum DeclaredKey {
    Index(i64),
    Str(Vec<u8>),
}

/// Recursive value encoder mirroring `ValueEncoder::encoderFor`.
enum Encoder {
    Integer,
    Float,
    Boolean,
    String,
    Null,
    Dynamic,
    DateTime,
    Interval,
    Uuid,
    Json,
    TimeZone,
    Enum,
    Xml,
    XmlElement,
    Html,
    HtmlElement,
    List(Box<Encoder>),
    Map(EncodeMapKey, Box<Encoder>),
    Structure(Vec<(DeclaredKey, Vec<u8>, Encoder)>, bool),
    Optional(Box<Encoder>),
}

fn build_encoder(type_json: &TypeJson) -> Result<Encoder, PhpException> {
    let missing = |part: &str| {
        ext_exception(format!(
            "flow_php schema JSON for type \"{}\" is missing its {part}",
            type_json.type_
        ))
    };

    Ok(match type_json.type_.as_str() {
        "integer" | "positive_integer" => Encoder::Integer,
        "float" => Encoder::Float,
        "boolean" => Encoder::Boolean,
        "string" | "non_empty_string" | "numeric-string" | "class_string" => Encoder::String,
        "null" => Encoder::Null,
        "mixed" | "union" | "scalar" | "literal" | "array" => Encoder::Dynamic,
        "datetime" | "date" => Encoder::DateTime,
        "time" => Encoder::Interval,
        "uuid" => Encoder::Uuid,
        "json" => Encoder::Json,
        "timezone" => Encoder::TimeZone,
        "enum" => Encoder::Enum,
        "xml" => Encoder::Xml,
        "xml_element" => Encoder::XmlElement,
        "html" => Encoder::Html,
        "html_element" => Encoder::HtmlElement,
        "list" => Encoder::List(Box::new(build_encoder(
            type_json.element().ok_or_else(|| missing("element"))?,
        )?)),
        "map" => {
            let key = match type_json
                .key()
                .ok_or_else(|| missing("key"))?
                .type_
                .as_str()
            {
                "integer" => EncodeMapKey::Integer,
                "string" => EncodeMapKey::String,
                _ => EncodeMapKey::Dynamic,
            };

            Encoder::Map(
                key,
                Box::new(build_encoder(
                    type_json.value().ok_or_else(|| missing("value"))?,
                )?),
            )
        }
        "structure" => {
            let mut elements = Vec::new();

            for (name, element) in type_json.all_elements() {
                let bytes = name.clone().into_bytes();
                let declared = match crate::ctx::array_key_index(&bytes) {
                    Some(index) => DeclaredKey::Index(index),
                    None => DeclaredKey::Str(bytes.clone()),
                };
                elements.push((declared, bytes, build_encoder(element)?));
            }

            Encoder::Structure(elements, type_json.allow_extra())
        }
        "optional" => Encoder::Optional(Box::new(build_encoder(
            type_json.base().ok_or_else(|| missing("base"))?,
        )?)),
        other => {
            return Err(ext_exception(format!(
                "flow_php does not support values of type \"{other}\" in this build"
            )));
        }
    })
}

struct EncodeColumn {
    name: Vec<u8>,
    value_slot: u32,
    definition_slot: u32,
    encoder: Encoder,
    /// (definition ce, `metadata` slot, `type` slot) - resolved lazily from the
    /// first definition instance seen for this column.
    definition_slots: Option<(*const ClassEntry, u32, u32)>,
}

pub struct EncodePlan {
    columns: Vec<EncodeColumn>,
}

/// `HASH_FLAG_PACKED` from zend_types.h.
const HASH_FLAG_PACKED: u8 = 1 << 2;

/// Raw order-preserving iteration over packed and bucket layouts, binary-safe
/// for arbitrary keys (ext-php-rs's `iter()` panics on non-UTF-8 string keys).
pub fn ht_for_each(
    ht: &ZendHashTable,
    mut f: impl FnMut(Option<&ZendStr>, zend_ulong, &Zval) -> Result<(), PhpException>,
) -> Result<(), PhpException> {
    let packed = unsafe { ht.u.v.flags } & HASH_FLAG_PACKED != 0;

    for index in 0..ht.nNumUsed {
        if packed {
            let value = unsafe { &*ht.__bindgen_anon_1.arPacked.add(index as usize) };

            if value.get_type() == ext_php_rs::flags::DataType::Undef {
                continue;
            }

            f(None, zend_ulong::from(index), value)?;
        } else {
            let bucket = unsafe { &*ht.__bindgen_anon_1.arData.add(index as usize) };

            if bucket.val.get_type() == ext_php_rs::flags::DataType::Undef {
                continue;
            }

            let key = unsafe { bucket.key.as_ref() };
            f(key, bucket.h, &bucket.val)?;
        }
    }

    Ok(())
}

pub(crate) fn read_slot(obj: &ZendObject, offset: u32) -> &Zval {
    unsafe {
        &*std::ptr::from_ref(obj)
            .cast::<u8>()
            .add(offset as usize)
            .cast::<Zval>()
    }
}

pub(crate) fn expect_object<'a>(
    zv: &'a Zval,
    context: &str,
) -> Result<&'a ZendObject, PhpException> {
    zv.object()
        .ok_or_else(|| ext_exception(format!("flow_php expected {context} to be an object")))
}

fn write_len_prefixed(out: &mut Vec<u8>, bytes: &[u8]) {
    write_u32(out, bytes.len() as u32);
    out.extend_from_slice(bytes);
}

/// Builds the encode plan from a SCHEMA frame body (the JSON the PHP
/// `SchemaEncoder` emits) - once built, no PHP call is needed to encode.
pub fn build_encode_plan(schema_json: &[u8]) -> Result<EncodePlan, PhpException> {
    let definitions = parse_schema_json(schema_json)?;

    let mut columns = Vec::with_capacity(definitions.len());

    for definition in &definitions {
        let entry_ce = entry_class_for_type(&definition.type_)?;

        columns.push(EncodeColumn {
            name: definition.name.clone().into_bytes(),
            value_slot: property_offset(entry_ce, "value")?,
            definition_slot: property_offset(entry_ce, "definition")?,
            encoder: build_encoder(&definition.type_)?,
            definition_slots: None,
        });
    }

    Ok(EncodePlan { columns })
}

/// Encodes one Row into a bare ROW frame body (no length prefix, no frame type).
pub fn encode_row_body(
    plan: &mut EncodePlan,
    row: &Zval,
    ctx: &mut Ctx,
) -> Result<Vec<u8>, PhpException> {
    let row_obj = expect_object(row, "a Row")?;
    let entries_obj = expect_object(read_slot(row_obj, ctx.row_entries_slot), "Row entries")?;
    let entries_ht = read_slot(entries_obj, ctx.entries_entries_slot)
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Entries to hold an array"))?;

    let mut out = Vec::with_capacity(1024);
    encode_row(plan, entries_ht, &mut out, ctx)?;

    Ok(out)
}

fn definition_slots(
    column: &mut EncodeColumn,
    definition: &ZendObject,
) -> Result<(u32, u32), PhpException> {
    let ce = definition.ce.cast_const();

    if let Some((cached_ce, metadata_slot, type_slot)) = column.definition_slots {
        if std::ptr::eq(cached_ce, ce) {
            return Ok((metadata_slot, type_slot));
        }
    }

    let ce_ref = unsafe { ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve a definition class"))?;
    let metadata_slot = property_offset(ce_ref, "metadata")?;
    let type_slot = property_offset(ce_ref, "type")?;
    column.definition_slots = Some((ce, metadata_slot, type_slot));

    Ok((metadata_slot, type_slot))
}

fn encode_row(
    plan: &mut EncodePlan,
    entries_ht: &ZendHashTable,
    out: &mut Vec<u8>,
    ctx: &mut Ctx,
) -> Result<(), PhpException> {
    for column in &mut plan.columns {
        let Some(entry_zv) = ht_find(entries_ht, &column.name) else {
            out.push(VALUE_ABSENT);

            continue;
        };

        let entry = expect_object(entry_zv, "a row entry")?;
        let value = read_slot(entry, column.value_slot);

        if value.is_null() {
            let definition =
                expect_object(read_slot(entry, column.definition_slot), "a definition")?;
            let (metadata_slot, _) = definition_slots(column, definition)?;
            let metadata =
                expect_object(read_slot(definition, metadata_slot), "definition metadata")?;
            let map = read_slot(metadata, ctx.metadata_map_slot()?)
                .array()
                .ok_or_else(|| ext_exception("flow_php expected Metadata to hold an array"))?;

            out.push(if crate::ctx::ht_contains(map, b"from_null") {
                VALUE_NULL_FROM_NULL
            } else {
                VALUE_NULL
            });
        } else {
            out.push(VALUE_PRESENT);
            encode_value(&column.encoder, value, out, ctx)?;
        }
    }

    Ok(())
}

fn encode_value(
    encoder: &Encoder,
    value: &Zval,
    out: &mut Vec<u8>,
    ctx: &mut Ctx,
) -> Result<(), PhpException> {
    match encoder {
        Encoder::Integer => out.extend_from_slice(
            &value
                .long()
                .ok_or_else(|| ext_exception("flow_php expected an integer value"))?
                .to_le_bytes(),
        ),
        Encoder::Float => out.extend_from_slice(
            &php_float(value)
                .ok_or_else(|| ext_exception("flow_php expected a float value"))?
                .to_le_bytes(),
        ),
        Encoder::Boolean => {
            out.push(u8::from(value.bool().ok_or_else(|| {
                ext_exception("flow_php expected a boolean value")
            })?))
        }
        Encoder::String => write_len_prefixed(
            out,
            value
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected a string value"))?
                .as_bytes(),
        ),
        Encoder::Null => {}
        Encoder::Dynamic => encode_dynamic(value, out, ctx)?,
        Encoder::DateTime => encode_datetime(expect_object(value, "a datetime value")?, out, ctx)?,
        Encoder::Interval => encode_interval(expect_object(value, "a time value")?, out, ctx)?,
        Encoder::Uuid => {
            let uuid = expect_object(value, "a uuid value")?;
            let (_, value_slot) = ctx.uuid()?;
            let bytes = read_slot(uuid, value_slot)
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected Uuid to hold a string"))?;
            out.extend_from_slice(bytes.as_bytes());
        }
        Encoder::Json => encode_json(expect_object(value, "a json value")?, out, ctx)?,
        Encoder::TimeZone => {
            let timezone = expect_object(value, "a timezone value")?;
            let name = call_handle_on(
                ctx.timezone_get_name()?,
                timezone,
                &mut [],
                "read a timezone name",
            )?;
            write_len_prefixed(
                out,
                name.zend_str()
                    .ok_or_else(|| ext_exception("flow_php expected a timezone name string"))?
                    .as_bytes(),
            );
        }
        Encoder::Enum => {
            let case = expect_object(value, "an enum value")?;
            let class = unsafe { case.ce.as_ref() }
                .and_then(ClassEntry::name)
                .ok_or_else(|| ext_exception("flow_php failed to resolve an enum class"))?;
            write_len_prefixed(out, class.as_bytes());

            let name = read_property(case, "name")?;
            write_len_prefixed(
                out,
                name.zend_str()
                    .ok_or_else(|| ext_exception("flow_php expected an enum case name"))?
                    .as_bytes(),
            );
        }
        Encoder::Xml => encode_via_static(value, out, ctx, "xmlDocumentToString")?,
        Encoder::XmlElement => encode_via_static(value, out, ctx, "xmlElementToString")?,
        Encoder::HtmlElement => encode_via_static(value, out, ctx, "htmlElementToString")?,
        Encoder::Html => {
            let document = expect_object(value, "an html value")?;
            let html = call_handle_on(
                ctx.save_html(document.ce.cast_const())?,
                document,
                &mut [],
                "convert an HTML document",
            )?;
            write_len_prefixed(
                out,
                html.zend_str()
                    .ok_or_else(|| ext_exception("flow_php expected saveHtml to return a string"))?
                    .as_bytes(),
            );
        }
        Encoder::List(element) => {
            let values = value
                .array()
                .ok_or_else(|| ext_exception("flow_php expected a list value"))?;
            write_u32(out, values.len() as u32);
            ht_for_each(values, |_, _, item| encode_value(element, item, out, ctx))?;
        }
        Encoder::Map(key, element) => {
            let values = value
                .array()
                .ok_or_else(|| ext_exception("flow_php expected a map value"))?;
            write_u32(out, values.len() as u32);
            ht_for_each(values, |map_key, index, item| {
                match key {
                    EncodeMapKey::Integer => {
                        let int_key = match map_key {
                            None => index as i64,
                            Some(_) => {
                                return Err(ext_exception("flow_php expected an integer map key"));
                            }
                        };
                        out.extend_from_slice(&int_key.to_le_bytes());
                    }
                    EncodeMapKey::String => match map_key {
                        // numeric-string keys coerced to int by the PHP array
                        // are cast back, like ValueEncoder
                        None => write_len_prefixed(out, (index as i64).to_string().as_bytes()),
                        Some(key) => write_len_prefixed(out, key.as_bytes()),
                    },
                    EncodeMapKey::Dynamic => match map_key {
                        None => {
                            out.push(TAG_INTEGER);
                            out.extend_from_slice(&(index as i64).to_le_bytes());
                        }
                        Some(key) => {
                            out.push(TAG_STRING);
                            write_len_prefixed(out, key.as_bytes());
                        }
                    },
                }

                encode_value(element, item, out, ctx)
            })?;
        }
        Encoder::Structure(elements, allow_extra) => {
            let values = value
                .array()
                .ok_or_else(|| ext_exception("flow_php expected a structure value"))?;

            for (declared, _, element) in elements {
                let item = match declared {
                    DeclaredKey::Index(index) => values.get_index(*index),
                    DeclaredKey::Str(key) => ht_find(values, key),
                };

                match item {
                    None => out.push(VALUE_ABSENT),
                    Some(item) if item.is_null() => out.push(VALUE_NULL),
                    Some(item) => {
                        out.push(VALUE_PRESENT);
                        encode_value(element, item, out, ctx)?;
                    }
                }
            }

            if *allow_extra {
                let is_declared = |key: Option<&ZendStr>, index: zend_ulong| {
                    elements
                        .iter()
                        .any(|(declared, _, _)| match (declared, key) {
                            (DeclaredKey::Index(declared_index), None) => {
                                *declared_index == index as i64
                            }
                            (DeclaredKey::Str(declared_key), Some(key)) => {
                                declared_key.as_slice() == key.as_bytes()
                            }
                            _ => false,
                        })
                };

                let mut extra_count = 0u32;
                ht_for_each(values, |key, index, _| {
                    if !is_declared(key, index) {
                        extra_count += 1;
                    }

                    Ok(())
                })?;
                write_u32(out, extra_count);

                ht_for_each(values, |key, index, item| {
                    if is_declared(key, index) {
                        return Ok(());
                    }

                    match key {
                        None => write_len_prefixed(out, (index as i64).to_string().as_bytes()),
                        Some(key) => write_len_prefixed(out, key.as_bytes()),
                    }

                    encode_dynamic(item, out, ctx)
                })?;
            }
        }
        Encoder::Optional(base) => {
            if value.is_null() {
                out.push(VALUE_NULL);
            } else {
                out.push(VALUE_PRESENT);
                encode_value(base, value, out, ctx)?;
            }
        }
    }

    Ok(())
}

/// Accepts ints where FloatType is declared, permissive like `pack('e')`.
fn php_float(value: &Zval) -> Option<f64> {
    value.double().or_else(|| value.long().map(|l| l as f64))
}

fn ht_find<'a>(ht: &'a ZendHashTable, key: &[u8]) -> Option<&'a Zval> {
    match crate::ctx::array_key_index(key) {
        Some(index) => ht.get_index(index),
        None => unsafe {
            ext_php_rs::ffi::zend_hash_str_find(
                std::ptr::from_ref(ht).cast_mut(),
                key.as_ptr().cast(),
                key.len(),
            )
            .as_ref()
        },
    }
}

/// Mirrors `ValueEncoder::encodeDynamic`.
fn encode_dynamic(value: &Zval, out: &mut Vec<u8>, ctx: &mut Ctx) -> Result<(), PhpException> {
    if value.is_null() {
        out.push(TAG_NULL);

        return Ok(());
    }

    if let Some(long) = value.long() {
        out.push(TAG_INTEGER);
        out.extend_from_slice(&long.to_le_bytes());

        return Ok(());
    }

    if let Some(double) = value.double() {
        out.push(TAG_FLOAT);
        out.extend_from_slice(&double.to_le_bytes());

        return Ok(());
    }

    if let Some(boolean) = value.bool() {
        out.push(TAG_BOOLEAN);
        out.push(u8::from(boolean));

        return Ok(());
    }

    if let Some(string) = value.zend_str() {
        out.push(TAG_STRING);
        write_len_prefixed(out, string.as_bytes());

        return Ok(());
    }

    if let Some(array) = value.array() {
        out.push(TAG_ARRAY);
        write_u32(out, array.len() as u32);

        return ht_for_each(array, |key, index, item| {
            match key {
                None => {
                    out.push(KEY_INTEGER);
                    out.extend_from_slice(&(index as i64).to_le_bytes());
                }
                Some(key) => {
                    out.push(KEY_STRING);
                    write_len_prefixed(out, key.as_bytes());
                }
            }

            encode_dynamic(item, out, ctx)
        });
    }

    if let Some(object) = value.object() {
        if object.instance_of(ctx.datetime_interface()?) {
            out.push(TAG_DATETIME);

            return encode_datetime(object, out, ctx);
        }

        let (uuid_ce, uuid_value_slot) = ctx.uuid()?;

        if std::ptr::eq(object.ce.cast_const(), std::ptr::from_ref(uuid_ce)) {
            out.push(TAG_UUID);
            out.extend_from_slice(
                read_slot(object, uuid_value_slot)
                    .zend_str()
                    .ok_or_else(|| ext_exception("flow_php expected Uuid to hold a string"))?
                    .as_bytes(),
            );

            return Ok(());
        }

        let (json_ce, ..) = ctx.json()?;

        if std::ptr::eq(object.ce.cast_const(), std::ptr::from_ref(json_ce)) {
            out.push(TAG_JSON);

            return encode_json(object, out, ctx);
        }
    }

    Err(ext_exception(format!(
        "flow_php does not support values of type \"{}\" in mixed/union context",
        debug_type(value)
    )))
}

fn debug_type(value: &Zval) -> String {
    if let Some(object) = value.object() {
        return unsafe { object.ce.as_ref() }
            .and_then(ClassEntry::name)
            .unwrap_or("object")
            .to_string();
    }

    format!("{:?}", value.get_type())
}

/// Mirrors `ValueEncoder::encodeDateTime`.
fn encode_datetime(
    value: &ZendObject,
    out: &mut Vec<u8>,
    ctx: &mut Ctx,
) -> Result<(), PhpException> {
    let ce = value.ce.cast_const();
    let immutable_ce = std::ptr::from_ref(ctx.datetime_fns(false)?.ce);
    let mutable_ce = std::ptr::from_ref(ctx.datetime_fns(true)?.ce);

    if std::ptr::eq(ce, immutable_ce) {
        out.push(DATETIME_IMMUTABLE);
    } else if std::ptr::eq(ce, mutable_ce) {
        out.push(DATETIME_MUTABLE);
    } else {
        let class = unsafe { ce.as_ref() }
            .and_then(ClassEntry::name)
            .ok_or_else(|| ext_exception("flow_php failed to resolve a datetime class"))?;

        return Err(ext_exception(format!(
            "Floe supports only DateTime and DateTimeImmutable, got {class} - convert custom datetime instances before writing"
        )));
    }

    let enc = ctx.datetime_encode_fns(ce)?;
    let get_timestamp = enc.get_timestamp;
    let format = enc.format;
    let get_timezone = enc.get_timezone;

    let timestamp = call_handle_on(get_timestamp, value, &mut [], "read a timestamp")?
        .long()
        .ok_or_else(|| ext_exception("flow_php expected getTimestamp to return an int"))?;

    let format_u = ctx.format_u()?.shallow_clone();
    let microseconds_zv =
        call_handle_on(format, value, &mut [format_u], "read datetime microseconds")?;
    let microseconds: u32 = microseconds_zv
        .zend_str()
        .and_then(|s| std::str::from_utf8(s.as_bytes()).ok())
        .and_then(|s| s.parse().ok())
        .ok_or_else(|| ext_exception("flow_php expected format('u') to return digits"))?;

    let timezone_zv = call_handle_on(get_timezone, value, &mut [], "read a timezone")?;
    let timezone = expect_object(&timezone_zv, "a timezone")?;
    let name = call_handle_on(
        ctx.timezone_get_name()?,
        timezone,
        &mut [],
        "read a timezone name",
    )?;

    out.extend_from_slice(&timestamp.to_le_bytes());
    write_u32(out, microseconds);
    write_len_prefixed(
        out,
        name.zend_str()
            .ok_or_else(|| ext_exception("flow_php expected a timezone name string"))?
            .as_bytes(),
    );

    Ok(())
}

/// Mirrors `ValueEncoder::encodeInterval`.
fn encode_interval(
    value: &ZendObject,
    out: &mut Vec<u8>,
    _ctx: &mut Ctx,
) -> Result<(), PhpException> {
    let read = |name: &str| -> Result<Zval, PhpException> { read_property(value, name) };

    let invert = read("invert")?
        .long()
        .ok_or_else(|| ext_exception("flow_php expected DateInterval::invert to be an int"))?;
    out.push(invert as u8);

    for field in ["y", "m", "d", "h", "i", "s"] {
        let field_value = read(field)?
            .long()
            .ok_or_else(|| ext_exception("flow_php expected DateInterval fields to be ints"))?;
        write_u32(out, field_value as u32);
    }

    let fraction = read("f")?;
    let fraction = php_float(&fraction)
        .ok_or_else(|| ext_exception("flow_php expected DateInterval::f to be a float"))?;
    out.extend_from_slice(&fraction.to_le_bytes());

    Ok(())
}

fn encode_json(value: &ZendObject, out: &mut Vec<u8>, ctx: &mut Ctx) -> Result<(), PhpException> {
    let (_, value_slot, is_object_slot) = ctx.json()?;

    write_len_prefixed(
        out,
        read_slot(value, value_slot)
            .zend_str()
            .ok_or_else(|| ext_exception("flow_php expected Json to hold a string"))?
            .as_bytes(),
    );
    out.push(u8::from(
        read_slot(value, is_object_slot)
            .bool()
            .ok_or_else(|| ext_exception("flow_php expected Json::isObject to be a bool"))?,
    ));

    Ok(())
}

/// Markup serialization delegated to the pure-PHP `ValueEncoder` public
/// statics - the exact code its closures run.
fn encode_via_static(
    value: &Zval,
    out: &mut Vec<u8>,
    ctx: &mut Ctx,
    method: &'static str,
) -> Result<(), PhpException> {
    let handle = ctx.value_encoder_static(method)?;
    let string = call_handle(
        handle,
        None,
        &mut [value.shallow_clone()],
        "serialize a markup value",
    )?;

    write_len_prefixed(
        out,
        string
            .zend_str()
            .ok_or_else(|| {
                ext_exception(format!(
                    "flow_php expected ValueEncoder::{method} to return a string"
                ))
            })?
            .as_bytes(),
    );

    Ok(())
}
