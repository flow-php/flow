use std::os::raw::{c_char, c_int};

use ext_php_rs::boxed::ZBox;
use ext_php_rs::exception::PhpException;
use ext_php_rs::flags::DataType;
use ext_php_rs::types::{ZendHashTable, ZendObject, ZendStr, Zval};
use ext_php_rs::zend::Function;

use crate::ctx::{
    array_key_index, call_handle, call_handle_catching, call_handle_on, call_handle_transparent,
    ce_method_ref, construct_with_zvals, ht_find_key, ht_insert, ht_insert_key, null_zval,
    schema_mismatch, transparent_exception, write_slot, zval_long, zval_str, Ctx, HtKey,
};
use crate::encode::{expect_object, ht_for_each, read_slot};
use crate::exception::ext_exception;
use crate::hydrate::{build_hydrate_plan, fold_metadata_into_schema, AssemblyClasses, HydratePlan, RowValuesClass};
use crate::plan::{parse_schema_json, TypeJson};
use crate::values::date_from_free_form;

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

pub(crate) enum MapKeyKind {
    Int,
    Str,
}

pub(crate) struct StructureElement {
    name: String,
    kind: CastKind,
    required: bool,
}

pub(crate) enum CastKind {
    Integer,
    PositiveInteger,
    Float,
    Boolean,
    String,
    NonEmptyString,
    DateTime,
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

fn build_cast_kind(type_json: &TypeJson) -> CastKind {
    match type_json.type_.as_str() {
        "integer" => CastKind::Integer,
        "positive_integer" => CastKind::PositiveInteger,
        "float" => CastKind::Float,
        "boolean" => CastKind::Boolean,
        "string" => CastKind::String,
        "non_empty_string" => CastKind::NonEmptyString,
        "datetime" => CastKind::DateTime,
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

struct CastColumn {
    kind: CastKind,
    /// The retained `$definition->type()` object - the fallback receiver.
    type_zv: Zval,
    /// `Type::cast` resolved on the concrete type class.
    cast_fn: &'static Function,
}

pub struct CastPlan {
    hydrate: HydratePlan,
    columns: Vec<CastColumn>,
}

fn build_cast_plan(schema: &Zval, ctx: &mut Ctx) -> Result<CastPlan, PhpException> {
    let hydrate = build_hydrate_plan(schema)?;

    let schema_obj = expect_object(schema, "a Schema")?;
    let schema_ce = unsafe { schema_obj.ce.as_ref() }
        .ok_or_else(|| ext_exception("flow_php failed to resolve the Schema class"))?;
    let normalize_fn = ce_method_ref(schema_ce, "normalize")?;
    let normalized = call_handle_on(normalize_fn, schema_obj, &mut [], "normalize a schema")?;

    let json = {
        let json_encode = ctx.json_encode()?;
        call_handle(
            json_encode,
            None,
            &mut [normalized.shallow_clone()],
            "encode a schema as JSON",
        )?
    };
    let json_bytes = json
        .zend_str()
        .ok_or_else(|| ext_exception("flow_php expected schema JSON to be a string"))?
        .as_bytes();

    let definitions = parse_schema_json(json_bytes)?;

    if definitions.len() != hydrate.columns.len() {
        return Err(ext_exception(
            "flow_php normalized schema JSON does not match the schema definitions",
        ));
    }

    let mut columns = Vec::with_capacity(definitions.len());

    for (definition, column) in definitions.iter().zip(&hydrate.columns) {
        if column.name_zv.zend_str().map(ZendStr::as_bytes) != Some(definition.name.as_bytes()) {
            return Err(ext_exception(
                "flow_php normalized schema JSON does not match the schema definitions",
            ));
        }

        let def_obj = column
            .base_def
            .object()
            .ok_or_else(|| ext_exception("flow_php expected a Definition object"))?;
        let def_ce = unsafe { def_obj.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a Definition class"))?;

        let type_fn = ce_method_ref(def_ce, "type")?;
        let type_zv = call_handle_on(type_fn, def_obj, &mut [], "read a definition type")?;
        let type_obj = type_zv
            .object()
            .ok_or_else(|| ext_exception("flow_php expected a definition type to be an object"))?;
        let type_ce = unsafe { type_obj.ce.as_ref() }
            .ok_or_else(|| ext_exception("flow_php failed to resolve a Type class"))?;

        columns.push(CastColumn {
            kind: build_cast_kind(&definition.type_),
            cast_fn: ce_method_ref(type_ce, "cast")?,
            type_zv,
        });
    }

    Ok(CastPlan { hydrate, columns })
}

/// The Schema `definitions` array address identifies the definition set;
/// retaining it (refcount++) prevents address reuse.
fn ensure_cast_plan(
    plan: &mut Option<CastPlan>,
    schema: &Zval,
    ctx: &mut Ctx,
) -> Result<(), PhpException> {
    let schema_obj = expect_object(schema, "a Schema")?;

    let current = plan.as_ref().and_then(|p| {
        read_slot(schema_obj, p.hydrate.definitions_slot)
            .array()
            .zip(p.hydrate.definitions_retained.array())
    });

    if current.is_some_and(|(definitions, retained)| std::ptr::eq(definitions, retained)) {
        return Ok(());
    }

    *plan = Some(build_cast_plan(schema, ctx)?);

    Ok(())
}

/// `HASH_FLAG_PACKED` from zend_types.h.
const HASH_FLAG_PACKED: u8 = 1 << 2;

/// In-order hashtable walk yielding `(string key, numeric key, value)` - the
/// early-exit sibling of `encode::ht_for_each` for the container cast checks.
struct HtEntries<'a> {
    ht: &'a ZendHashTable,
    packed: bool,
    index: u32,
}

fn ht_entries(ht: &ZendHashTable) -> HtEntries<'_> {
    HtEntries {
        ht,
        packed: unsafe { ht.u.v.flags } & HASH_FLAG_PACKED != 0,
        index: 0,
    }
}

impl<'a> Iterator for HtEntries<'a> {
    type Item = (Option<&'a ZendStr>, u64, &'a Zval);

    fn next(&mut self) -> Option<Self::Item> {
        while self.index < self.ht.nNumUsed {
            let position = self.index as usize;
            self.index += 1;

            if self.packed {
                let value = unsafe { &*self.ht.__bindgen_anon_1.arPacked.add(position) };

                if value.get_type() == DataType::Undef {
                    continue;
                }

                return Some((None, position as u64, value));
            }

            let bucket = unsafe { &*self.ht.__bindgen_anon_1.arData.add(position) };

            if bucket.val.get_type() == DataType::Undef {
                continue;
            }

            return Some((unsafe { bucket.key.as_ref() }, bucket.h, &bucket.val));
        }

        None
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
fn bool_from_str(bytes: &[u8]) -> Option<bool> {
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

/// The `Uuid::UUID_REGEXP` pattern: 8-4-4-4-12 LOWERCASE hex groups.
fn is_uuid(bytes: &[u8]) -> bool {
    if bytes.len() != 36 {
        return false;
    }

    bytes.iter().enumerate().all(|(index, byte)| match index {
        8 | 13 | 18 | 23 => *byte == b'-',
        _ => matches!(byte, b'0'..=b'9' | b'a'..=b'f'),
    })
}

/// The cheap prefix of `Json::isValid`: non-empty + matching `{}`/`[]` pair.
fn json_gate(bytes: &[u8]) -> bool {
    bytes.len() >= 2
        && ((bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}')
            || (bytes[0] == b'[' && bytes[bytes.len() - 1] == b']'))
}

fn json_object_from(bytes: &[u8], ctx: &mut Ctx) -> Result<Zval, PhpException> {
    let (json_ce, value_slot, is_object_slot) = ctx.json()?;
    let mut json = ZendObject::new(json_ce);
    write_slot(&mut json, value_slot, zval_str(bytes));

    let mut is_object_zv = Zval::new();
    is_object_zv.set_bool(bytes[0] == b'{' && bytes[bytes.len() - 1] == b'}');
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
fn is_numeric_str(bytes: &[u8]) -> (u8, i64, f64) {
    let mut lval: i64 = 0;
    let mut dval: f64 = 0.0;

    let code = unsafe {
        _is_numeric_string_ex(
            bytes.as_ptr().cast::<c_char>(),
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

/// Casts one value natively; `Ok(None)` bails the WHOLE column value to the
/// PHP fallback (`Type::cast`), which reproduces results, exception classes,
/// messages and `previous` chains by re-running the canonical implementation.
/// PHP calls made inside a branch discard their own thrown exceptions and bail
/// instead - mirroring the `catch (Throwable)` wrappers in the PHP casts.
fn cast_value(kind: &CastKind, value: &Zval, ctx: &mut Ctx) -> Result<Option<Zval>, PhpException> {
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
                let bytes = string.as_bytes();

                match is_numeric_str(bytes) {
                    (IS_LONG, long, _) => Some(zval_long(long)),
                    (IS_DOUBLE, _, _) if integer_shaped(bytes) => None,
                    (IS_DOUBLE, _, double) if fits_i64(double) => Some(zval_long(double as i64)),
                    _ => None,
                }
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
            } else if value.is_long() || value.is_double() {
                let mut zv = Zval::new();
                zv.set_zend_string(engine_string(value)?);
                Some(zv)
            } else if value.is_true() || value.is_false() {
                Some(ctx.bool_str(value.is_true())?.shallow_clone())
            } else {
                None
            }
        }
        CastKind::NonEmptyString => {
            if value.zend_str().is_some_and(|s| !s.as_bytes().is_empty()) {
                Some(value.shallow_clone())
            } else if value.is_long() || value.is_double() {
                // engine string form of a number is never empty
                let mut zv = Zval::new();
                zv.set_zend_string(engine_string(value)?);
                Some(zv)
            } else if value.is_true() || value.is_false() {
                Some(ctx.bool_str(value.is_true())?.shallow_clone())
            } else {
                None
            }
        }
        CastKind::DateTime => {
            if let Some(object) = value.object() {
                let immutable_ce = ctx.datetime_fns(false)?.ce;

                if object.instance_of(immutable_ce) {
                    Some(value.shallow_clone())
                } else {
                    None
                }
            } else if value.is_string() {
                // DateTimeType::cast gates a string on StringTemporalParts, whose recogniser is a PHP
                // class; a format whitelist here would be a second grammar, so the string bails whole
                None
            } else if value.is_long() || value.is_double() {
                date_from_free_form(&timestamp_string(value)?, ctx)?
            } else {
                None
            }
        }
        CastKind::Date => cast_date(value, ctx)?,
        CastKind::Uuid => {
            if let Some(object) = value.object() {
                let (uuid_ce, _) = ctx.uuid()?;

                if object.instance_of(uuid_ce) {
                    Some(value.shallow_clone())
                } else {
                    None
                }
            } else if let Some(string) = value.zend_str() {
                if is_uuid(string.as_bytes()) {
                    let (uuid_ce, value_slot) = ctx.uuid()?;
                    let mut uuid = ZendObject::new(uuid_ce);
                    write_slot(&mut uuid, value_slot, zval_str(string.as_bytes()));

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
        CastKind::Json => cast_json(value, ctx)?,
        CastKind::List(inner) => {
            let Some(values) = value.array() else {
                return Ok(None);
            };

            let mut out = ZendHashTable::with_capacity(values.len() as u32);

            for (expected, (string_key, index, item)) in ht_entries(values).enumerate() {
                if string_key.is_some() || index != expected as u64 {
                    return Ok(None);
                }

                let Some(casted) = cast_value(inner, item, ctx)? else {
                    return Ok(None);
                };

                out.push(casted).map_err(|e| {
                    ext_exception(format!("flow_php failed to collect list values: {e:?}"))
                })?;
            }

            let mut zv = Zval::new();
            zv.set_hashtable(out);
            Some(zv)
        }
        CastKind::Map(key_kind, value_kind) => {
            let Some(values) = value.array() else {
                return Ok(None);
            };

            let mut out = ZendHashTable::with_capacity(values.len() as u32);

            for (string_key, index, item) in ht_entries(values) {
                let key = match (key_kind, string_key) {
                    (MapKeyKind::Int, None) => HtKey::Index(index as i64),
                    (MapKeyKind::Str, Some(name)) => HtKey::Str(name),
                    _ => return Ok(None),
                };

                let Some(casted) = cast_value(value_kind, item, ctx)? else {
                    return Ok(None);
                };

                ht_insert_key(&mut out, &key, casted);
            }

            let mut zv = Zval::new();
            zv.set_hashtable(out);
            Some(zv)
        }
        CastKind::Structure(elements) => {
            let Some(values) = value.array() else {
                return Ok(None);
            };

            let mut out = ZendHashTable::with_capacity(elements.len() as u32);
            let mut matched = 0usize;

            for element in elements {
                let Some(item) = values.get(element.name.as_str()) else {
                    if element.required {
                        // PHP throws MissingElementCastingException for absent required elements
                        return Ok(None);
                    }

                    continue;
                };

                if item.is_null() && !matches!(element.kind, CastKind::Optional(_)) {
                    // PHP throws MissingElementCastingException for present-null elements
                    // whose type rejects null - required and structure-level optional alike
                    return Ok(None);
                }

                let Some(casted) = cast_value(&element.kind, item, ctx)? else {
                    return Ok(None);
                };

                ht_insert(&mut out, element.name.as_bytes(), casted);
                matched += 1;
            }

            if matched == 0 {
                // an all-optional structure with no matched keys fails PHP's assert
                return Ok(None);
            }

            let mut zv = Zval::new();
            zv.set_hashtable(out);
            Some(zv)
        }
        CastKind::Optional(inner) => {
            if value.is_null() {
                Some(null_zval())
            } else {
                cast_value(inner, value, ctx)?
            }
        }
        CastKind::Fallback => None,
    })
}

/// `"@" . $value` with the engine's number-to-string conversion.
fn timestamp_string(value: &Zval) -> Result<Vec<u8>, PhpException> {
    let number = engine_string(value)?;
    let mut bytes = Vec::with_capacity(number.as_bytes().len() + 1);
    bytes.push(b'@');
    bytes.extend_from_slice(number.as_bytes());

    Ok(bytes)
}

fn cast_date(value: &Zval, ctx: &mut Ctx) -> Result<Option<Zval>, PhpException> {
    if let Some(object) = value.object() {
        let immutable_ce = ctx.datetime_fns(false)?.ce;

        if !object.instance_of(immutable_ce) {
            return Ok(None);
        }

        let fns = ctx.datetime_cast_fns(object.ce.cast_const())?;
        let (set_time, format) = (fns.set_time, fns.format);
        let his = ctx.format_his()?.shallow_clone();

        // isValid passthrough: midnight keeps the SAME object (serialize
        // back-reference topology); a thrown format() bails like PHP's catch
        let Ok(formatted) = call_handle_transparent(format, Some(object), &mut [his]) else {
            return Ok(None);
        };

        if formatted.zend_str().map(ZendStr::as_bytes) == Some(b"00:00:00") {
            return Ok(Some(value.shallow_clone()));
        }

        return set_midnight(set_time, object);
    }

    if value.is_string() {
        // see CastKind::DateTime above - the string gate lives in PHP
        return Ok(None);
    }

    let parsed = if value.is_long() || value.is_double() {
        date_from_free_form(&timestamp_string(value)?, ctx)?
    } else {
        None
    };

    let Some(datetime) = parsed else {
        return Ok(None);
    };
    let object = datetime
        .object()
        .ok_or_else(|| ext_exception("flow_php expected a datetime object"))?;
    let set_time = ctx.datetime_cast_fns(object.ce.cast_const())?.set_time;

    set_midnight(set_time, object)
}

fn set_midnight(
    set_time: &'static Function,
    object: &ZendObject,
) -> Result<Option<Zval>, PhpException> {
    let mut args = [zval_long(0), zval_long(0), zval_long(0), zval_long(0)];

    let Ok(result) = call_handle_transparent(set_time, Some(object), &mut args) else {
        return Ok(None);
    };

    if !result.is_object() {
        return Ok(None);
    }

    Ok(Some(result))
}

fn cast_json(value: &Zval, ctx: &mut Ctx) -> Result<Option<Zval>, PhpException> {
    if let Some(object) = value.object() {
        let (json_ce, _, _) = ctx.json()?;

        if object.instance_of(json_ce) {
            return Ok(Some(value.shallow_clone()));
        }

        return Ok(None);
    }

    if let Some(string) = value.zend_str() {
        if !json_gate(string.as_bytes()) {
            return Ok(None);
        }

        let json_validate = ctx.json_validate()?;
        let Ok(valid) =
            call_handle_transparent(json_validate, None, &mut [value.shallow_clone()])
        else {
            return Ok(None);
        };

        if !valid.bool().unwrap_or(false) {
            return Ok(None);
        }

        return Ok(Some(json_object_from(string.as_bytes(), ctx)?));
    }

    if value.is_array() {
        // Json::fromArray's json_encode, in its non-throwing flavor: `false`
        // (or a thrown JsonSerializable) bails to the PHP cast
        let json_encode = ctx.json_encode()?;
        let Ok(encoded) = call_handle_transparent(json_encode, None, &mut [value.shallow_clone()])
        else {
            return Ok(None);
        };

        let Some(string) = encoded.zend_str() else {
            return Ok(None);
        };

        if !json_gate(string.as_bytes()) {
            return Ok(None);
        }

        return Ok(Some(json_object_from(string.as_bytes(), ctx)?));
    }

    Ok(None)
}

/// Native `PhpRowHydrator::hydrate`: raw scalars cast against a `Schema` and
/// assembled into `Flow\ETL\Rows` in one pass. Column values cast natively
/// where proven identical, otherwise per value through the retained PHP
/// `Type::cast`; a null under a nullable declaration passes through, and both an
/// absence and a present null under NOT NULL are refused - each with its own cause.
pub fn cast_rows(
    batch: &Zval,
    schema: &Zval,
    plan_slot: &mut Option<CastPlan>,
    raw_class: &RowValuesClass,
    assembly: &AssemblyClasses,
    ctx: &mut Ctx,
) -> Result<Zval, PhpException> {
    ensure_cast_plan(plan_slot, schema, ctx)?;
    let plan = plan_slot.as_ref().expect("plan built above");

    let batch_ht = batch
        .array()
        .ok_or_else(|| ext_exception("flow_php expected a list of raw row values"))?;

    let schema_zv = fold_metadata_into_schema(schema, batch_ht, raw_class, ctx)?;
    let mut rows_ht = ZendHashTable::with_capacity(batch_ht.len() as u32);

    ht_for_each(batch_ht, |_, row_index, rv_zv| {
        let rv = expect_object(rv_zv, "a RawRowValues")?;
        let values_ht = read_slot(rv, raw_class.values_slot)
            .array()
            .ok_or_else(|| ext_exception("flow_php expected RawRowValues::values to be an array"))?;

        let mut row_values = ZendHashTable::with_capacity(plan.hydrate.columns.len() as u32);

        for (column, cast_column) in plan.hydrate.columns.iter().zip(&plan.columns) {
            let key = column.key();

            // An absent column is simply not inserted, mirroring HydratedBatch's `continue`:
            // Rows::__construct runs Row::matchTo(), which pads a declared-nullable absence and
            // raises missingColumn for a NOT-NULL one. Re-implementing that here instead would
            // refuse the absence mid-row, so a later cast refusal in the same batch would be
            // reported by PHP and pre-empted by native - the two paths would name different
            // columns and different rows for the same input.
            let Some(value) = ht_find_key(values_ht, &key) else {
                continue;
            };

            // a present null is a DIFFERENT refusal from an absence: valueDoesNotMatch, not
            // missingColumn, and 027 pins the two messages apart
            if value.is_null() && !column.nullable {
                return Err(schema_mismatch(
                    assembly.schema_mismatch_ce,
                    assembly.value_does_not_match,
                    row_index,
                    &mut [column.base_def.shallow_clone(), null_zval()],
                )?);
            }

            let casted = if value.is_null() {
                null_zval()
            } else {
                match cast_value(&cast_column.kind, value, ctx)? {
                    Some(casted) => casted,
                    None => {
                        let type_obj = cast_column
                            .type_zv
                            .object()
                            .ok_or_else(|| ext_exception("flow_php expected a Type object"))?;

                        match call_handle_catching(
                            cast_column.cast_fn,
                            Some(type_obj),
                            &mut [value.shallow_clone()],
                        ) {
                            Ok(casted) => casted,
                            Err(mut refusal) => {
                                if !refusal.instance_of(assembly.types_exception_ce) {
                                    return Err(transparent_exception(&mut refusal));
                                }

                                // The declared type refused the value. Its own exception is dropped
                                // rather than chained, exactly as HydratedBatch's guard drops it -
                                // 027 compares the two hydrators' class and message byte for byte.
                                return Err(schema_mismatch(
                                    assembly.schema_mismatch_ce,
                                    assembly.value_does_not_match,
                                    row_index,
                                    &mut [column.base_def.shallow_clone(), value.shallow_clone()],
                                )?);
                            }
                        }
                    }
                }
            };

            ht_insert_key(&mut row_values, &key, casted);
        }

        let mut values_zv = Zval::new();
        values_zv.set_hashtable(row_values);

        let mut row = construct_with_zvals(assembly.row_ce, &mut [values_zv], "a Row")?;
        let mut row_zv = Zval::new();
        row_zv.set_object(&mut row);
        rows_ht.push(row_zv).map_err(|e| {
            ext_exception(format!("flow_php failed to collect hydrated rows: {e:?}"))
        })?;

        Ok(())
    })?;

    let mut rows_zv = Zval::new();
    rows_zv.set_hashtable(rows_ht);

    // every non-null value was just cast to its column's type, so the batch takes
    // the shape-only door - the one HydratedBatch returns through
    call_handle(assembly.rows_conformed, None, &mut [schema_zv, rows_zv], "conform Rows")
}
