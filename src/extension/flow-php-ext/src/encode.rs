//! Floe ROW frame-body encoder mirroring `Flow\Floe\PhpFloeEncoder::encode`
//! byte-for-byte; values and per-value metadata are read from a `TypedRowValues`.

use ext_php_rs::exception::PhpException;
use ext_php_rs::ffi::zend_ulong;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::{ClassEntry, Function};

use crate::ctx::{
    call_handle, call_handle_on, ce_method_ref, find_class, null_zval, read_property,
    schema_mismatch, zval_str, Ctx,
};
use crate::exception::ext_exception;
use crate::format::{
    write_u32, VALUE_ABSENT, VALUE_NULL, VALUE_NULL_WITH_META, VALUE_PRESENT, VALUE_PRESENT_WITH_META,
};
use crate::plan::{parse_schema_json, TypeJson};

enum EncodeMapKey {
    Integer,
    String,
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
    Structure(Vec<(DeclaredKey, Vec<u8>, Encoder)>),
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
                other => {
                    return Err(ext_exception(format!(
                        "flow_php does not support map keys of type \"{other}\""
                    )));
                }
            };

            Encoder::Map(
                key,
                Box::new(build_encoder(
                    type_json.value().ok_or_else(|| missing("value"))?,
                )?),
            )
        }
        "structure_v2" => {
            if type_json.fields().is_empty() {
                return Err(ext_exception(
                    "flow_php read a structure type with no fields; the loaded flow_php extension and the \
                     flow-php/etl in use disagree on the structure schema format - reinstall one to match the \
                     other, or set the Floe engine to FloeEngine::php",
                ));
            }

            let mut elements = Vec::new();

            for field in type_json.fields() {
                let bytes = field.name.clone().into_bytes();
                let declared = match crate::ctx::array_key_index(&bytes) {
                    Some(index) => DeclaredKey::Index(index),
                    None => DeclaredKey::Str(bytes.clone()),
                };
                elements.push((declared, bytes, build_encoder(&field.type_)?));
            }

            if type_json.allow_extra() {
                return Err(ext_exception(
                    "flow_php does not support structures that allow extra values",
                ));
            }

            Encoder::Structure(elements)
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

pub(crate) struct EncodeColumn {
    pub(crate) name: Vec<u8>,
    encoder: Encoder,
    /// `Definition::isNullable()`, read once at plan build - never per row
    nullable: bool,
    /// retained (refcount++) so a refusal can be raised through the PHP factory,
    /// which is the only place a schema-mismatch message is ever authored
    base_def: Zval,
    /// Canonical PHP `json_encode` of this column's section-schema metadata - the
    /// divergence reference. An entry whose metadata JSON differs rides its own
    /// metadata beside the value.
    plan_metadata_json: Vec<u8>,
    plan_metadata_empty: bool,
}

pub struct EncodePlan {
    pub(crate) columns: Vec<EncodeColumn>,
    schema_mismatch_ce: &'static ClassEntry,
    value_does_not_match: &'static Function,
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
/// `SchemaEncoder` emits). Each column's section-schema metadata is captured as
/// its canonical PHP `json_encode` so a diverging entry can be detected without
/// re-parsing the schema per row.
pub fn build_encode_plan(
    schema_json: &[u8],
    schema: &Zval,
    ctx: &mut Ctx,
) -> Result<EncodePlan, PhpException> {
    let definitions = parse_schema_json(schema_json)?;
    let base_defs = schema_definitions(schema)?;

    let mut assoc_zv = Zval::new();
    assoc_zv.set_bool(true);
    let decoded = {
        let json_decode = ctx.json_decode()?;
        call_handle(
            json_decode,
            None,
            &mut [zval_str(schema_json), assoc_zv],
            "decode a schema frame for metadata",
        )?
    };
    let decoded_ht = decoded
        .array()
        .ok_or_else(|| ext_exception("flow_php expected a schema frame to decode to a list"))?;

    let mut columns = Vec::with_capacity(definitions.len());

    for (index, definition) in definitions.iter().enumerate() {
        let plan_metadata_json = column_metadata_json(decoded_ht, index, ctx)?;

        let base_def = base_defs
            .get(index)
            .ok_or_else(|| ext_exception("flow_php expected a Definition for every schema column"))?
            .shallow_clone();

        columns.push(EncodeColumn {
            name: definition.name.clone().into_bytes(),
            encoder: build_encoder(&definition.type_)?,
            nullable: definition.nullable,
            base_def,
            plan_metadata_empty: plan_metadata_json == b"[]",
            plan_metadata_json,
        });
    }

    Ok(EncodePlan {
        columns,
        schema_mismatch_ce: find_class("Flow\\ETL\\Exception\\SchemaMismatchException")?,
        value_does_not_match: ce_method_ref(
            find_class("Flow\\ETL\\Exception\\ColumnMismatchException")?,
            "valueDoesNotMatch",
        )?,
    })
}

/// Canonical PHP `json_encode` of the `metadata` map of the `index`th decoded
/// schema definition (`[]` when absent).
fn column_metadata_json(
    decoded_ht: &ZendHashTable,
    index: usize,
    ctx: &mut Ctx,
) -> Result<Vec<u8>, PhpException> {
    let definition_ht = decoded_ht
        .get_index(index as i64)
        .and_then(Zval::array)
        .ok_or_else(|| ext_exception("flow_php expected a schema definition to be a map"))?;

    let metadata_zv = match definition_ht.get("metadata") {
        Some(zv) => zv.shallow_clone(),
        None => return Ok(b"[]".to_vec()),
    };

    let json_encode = ctx.json_encode()?;
    let encoded = call_handle(
        json_encode,
        None,
        &mut [metadata_zv],
        "encode section-schema metadata",
    )?;

    encoded
        .zend_str()
        .map(|s| s.as_bytes().to_vec())
        .ok_or_else(|| ext_exception("flow_php expected json_encode to return a string"))
}

/// The `Schema`'s own `Definition` objects in declaration order - the same walk
/// `build_hydrate_plan` does, and the same order `parse_schema_json` yields.
/// Runs once per schema rebind, never per row.
fn schema_definitions(schema: &Zval) -> Result<Vec<Zval>, PhpException> {
    let schema_obj = expect_object(schema, "a Schema")?;
    let schema_ce = unsafe { schema_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve the Schema class"))?;
    let definitions = call_handle_on(
        ce_method_ref(schema_ce, "definitions")?,
        schema_obj,
        &mut [],
        "read schema definitions",
    )?;
    let definitions_ht = definitions
        .array()
        .ok_or_else(|| ext_exception("flow_php expected Schema::definitions to return an array"))?;

    let mut base_defs = Vec::with_capacity(definitions_ht.len());

    ht_for_each(definitions_ht, |_, _, def_zv| {
        base_defs.push(def_zv.shallow_clone());

        Ok(())
    })?;

    Ok(base_defs)
}

/// Encodes one `Flow\ETL\Row\TypedRowValues` (its `values` + `metadata` maps)
/// into a bare ROW frame body (no length prefix, no frame type). Byte-identical
/// to `Flow\Floe\PhpFloeEncoder::encode` for the same row.
pub fn encode_typed_row(
    plan: &EncodePlan,
    row_index: u64,
    values_ht: &ZendHashTable,
    metadata_ht: &ZendHashTable,
    ctx: &mut Ctx,
) -> Result<Vec<u8>, PhpException> {
    let mut out = Vec::with_capacity(1024);

    for column in &plan.columns {
        let Some(value) = ht_find(values_ht, &column.name) else {
            return Err(ext_exception(format!(
                "flow_php found a row that does not carry the declared column \"{}\"",
                String::from_utf8_lossy(&column.name)
            )));
        };

        let (diverges, entry_metadata_json) = typed_metadata(column, metadata_ht, ctx)?;

        if value.is_null() {
            if !column.nullable {
                return Err(schema_mismatch(
                    plan.schema_mismatch_ce,
                    plan.value_does_not_match,
                    row_index,
                    &mut [column.base_def.shallow_clone(), null_zval()],
                )?);
            }

            if diverges {
                out.push(VALUE_NULL_WITH_META);
                write_len_prefixed(&mut out, &entry_metadata_json);
            } else {
                out.push(VALUE_NULL);
            }

            continue;
        }

        if diverges {
            out.push(VALUE_PRESENT_WITH_META);
            write_len_prefixed(&mut out, &entry_metadata_json);
        } else {
            out.push(VALUE_PRESENT);
        }

        encode_value(&column.encoder, value, &mut out, ctx)?;
    }

    Ok(out)
}

/// Whether a column's per-value metadata diverges from the section-schema
/// reference and, if so, its canonical JSON (the beside-value blob). `metadata_ht`
/// is the `TypedRowValues::metadata` map, which carries only non-empty entries
/// (mirroring `PhpRowHydrator::dehydrate`); an absent key is empty metadata. Fast
/// path: an empty entry against an empty-metadata column never diverges - no PHP call.
fn typed_metadata(
    column: &EncodeColumn,
    metadata_ht: &ZendHashTable,
    ctx: &mut Ctx,
) -> Result<(bool, Vec<u8>), PhpException> {
    let metadata_zv = ht_find(metadata_ht, &column.name);

    let entry_empty = match metadata_zv {
        None => true,
        Some(zv) => {
            let metadata = expect_object(zv, "per-value metadata")?;
            let map_slot = ctx.metadata_map_slot()?;
            read_slot(metadata, map_slot)
                .array()
                .is_none_or(|map| map.is_empty())
        }
    };

    if entry_empty && column.plan_metadata_empty {
        return Ok((false, Vec::new()));
    }

    let entry_metadata_json = if entry_empty {
        b"[]".to_vec()
    } else {
        let metadata = expect_object(
            metadata_zv.expect("Some when the entry is not empty"),
            "per-value metadata",
        )?;
        let map_slot = ctx.metadata_map_slot()?;
        let map_owned = read_slot(metadata, map_slot).shallow_clone();
        let json_encode = ctx.json_encode()?;
        let encoded = call_handle(
            json_encode,
            None,
            &mut [map_owned],
            "encode per-value metadata",
        )?;

        encoded
            .zend_str()
            .map(|s| s.as_bytes().to_vec())
            .ok_or_else(|| ext_exception("flow_php expected json_encode to return a string"))?
    };

    let diverges = entry_metadata_json != column.plan_metadata_json;

    Ok((diverges, entry_metadata_json))
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
        Encoder::DateTime => encode_datetime(expect_object(value, "a datetime value")?, out, ctx)?,
        Encoder::Interval => encode_interval(expect_object(value, "a time value")?, out, ctx)?,
        Encoder::Uuid => {
            let uuid = expect_object(value, "a uuid value")?;
            let (uuid_ce, value_slot) = ctx.uuid()?;

            if !std::ptr::eq(uuid.ce.cast_const(), std::ptr::from_ref(uuid_ce)) {
                return Err(ext_exception("flow_php expected a uuid value to be an object"));
            }

            let bytes = read_slot(uuid, value_slot)
                .zend_str()
                .ok_or_else(|| ext_exception("flow_php expected Uuid to hold a string"))?;
            write_len_prefixed(out, bytes.as_bytes());
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
                }

                encode_value(element, item, out, ctx)
            })?;
        }
        Encoder::Structure(elements) => {
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

/// Mirrors `ValueEncoder::encodeDateTime`.
fn encode_datetime(
    value: &ZendObject,
    out: &mut Vec<u8>,
    ctx: &mut Ctx,
) -> Result<(), PhpException> {
    let ce = value.ce.cast_const();
    let immutable_ce = std::ptr::from_ref(ctx.datetime_fns(false)?.ce);
    let mutable_ce = std::ptr::from_ref(ctx.datetime_fns(true)?.ce);

    // the class is not stored - a datetime column always hydrates to DateTimeImmutable - but a
    // custom subclass is still refused, because its extra state would be dropped silently
    if !std::ptr::eq(ce, immutable_ce) && !std::ptr::eq(ce, mutable_ce) {
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
    let (json_ce, value_slot, is_object_slot) = ctx.json()?;

    if !std::ptr::eq(value.ce.cast_const(), std::ptr::from_ref(json_ce)) {
        return Err(ext_exception("flow_php expected a json value to be an object"));
    }

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
