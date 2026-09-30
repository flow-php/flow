//! The `Flow\Parquet` lib's per-cell values over the core's canonical arrays: what `PhpParquetEngine` returns for a
//! Parquet value on read, and what it accepts per target type on write.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{make_array, Array, ArrayRef};
use arrow_schema::{DataType, Field};
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};
use ext_php_rs::zend::{ClassEntry, Function};
use flow_batch_frame::kind::{Field as KindField, Kind};

use crate::ctx::{
    call_handle_transparent, ce_method_ref, find_class, ht_for_each, ht_get, ht_insert, ht_insert_long,
    read_property, zval_long, zval_str,
};
use crate::exception::ext_exception;
use crate::kind_builder::KindBuilder;
use crate::parquet::error::Error;
use crate::values::{date_from_days, datetime_days, datetime_from_micros, datetime_micros, uuid_bytes, uuid_text};

const UUID: &str = "arrow.uuid";

fn is_uuid(field: &Field) -> bool {
    field.extension_type_name() == Some(UUID)
}

fn unsupported(data_type: &DataType) -> Error {
    Error::Unsupported {
        column: String::new(),
        parquet: data_type.to_string(),
    }
}

fn collected(error: impl std::fmt::Debug) -> PhpException {
    ext_exception(format!("flow_php failed to collect a Parquet value: {error:?}"))
}

/// How a canonical array's values become PHP values, per `PhpParquetEngine`.
pub enum ReadCell {
    Null,
    Bool,
    Int,
    Float,
    Bytes,
    /// `FixedSizeBinary(16)` under `arrow.uuid`: the lowercase 36-char text.
    Uuid,
    Date,
    Timestamp,
    Time,
    List(Box<ReadCell>),
    Map(Box<ReadCell>, Box<ReadCell>),
    Struct(Vec<(Vec<u8>, ReadCell)>),
}

impl ReadCell {
    /// `canonical`: the type the core reads the column as; `parquet`: the field it was read from (its extension type
    /// tells a uuid from 16 bytes), walked in parallel with the canonical type's children.
    pub fn new(canonical: &DataType, parquet: &Field) -> Result<Self, Error> {
        Ok(match canonical {
            DataType::Null => ReadCell::Null,
            DataType::Boolean => ReadCell::Bool,
            DataType::Int64 => ReadCell::Int,
            DataType::Float64 => ReadCell::Float,
            DataType::Binary => ReadCell::Bytes,
            DataType::FixedSizeBinary(16) if is_uuid(parquet) => ReadCell::Uuid,
            DataType::FixedSizeBinary(16) => ReadCell::Bytes,
            DataType::Date32 => ReadCell::Date,
            DataType::Timestamp(_, _) => ReadCell::Timestamp,
            DataType::Duration(_) => ReadCell::Time,
            DataType::List(element) => ReadCell::List(Box::new(Self::new(element.data_type(), child(parquet, 0)?)?)),
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(unsupported(canonical));
                };
                let parquet_entries = child(parquet, 0)?;
                let key = Self::new(fields[0].data_type(), child(parquet_entries, 0)?)?;

                if !matches!(key, ReadCell::Int | ReadCell::Bytes | ReadCell::Uuid) {
                    return Err(Error::MapKey {
                        column: String::new(),
                        key: key.php_type(),
                    });
                }

                ReadCell::Map(
                    Box::new(key),
                    Box::new(Self::new(fields[1].data_type(), child(parquet_entries, 1)?)?),
                )
            }
            DataType::Struct(fields) => ReadCell::Struct(
                fields
                    .iter()
                    .enumerate()
                    .map(|(index, field)| {
                        Ok((field.name().as_bytes().to_vec(), Self::new(field.data_type(), child(parquet, index)?)?))
                    })
                    .collect::<Result<_, Error>>()?,
            ),
            other => return Err(unsupported(other)),
        })
    }

    /// The PHP type this cell's values have: what a MAP key refusal names.
    fn php_type(&self) -> &'static str {
        match self {
            ReadCell::Null => "null",
            ReadCell::Bool => "bool",
            ReadCell::Int => "int",
            ReadCell::Float => "float",
            ReadCell::Bytes | ReadCell::Uuid => "string",
            ReadCell::Date | ReadCell::Timestamp => "DateTimeImmutable",
            ReadCell::Time => "DateInterval",
            ReadCell::List(_) | ReadCell::Map(_, _) | ReadCell::Struct(_) => "array",
        }
    }

    pub fn needs_times(&self) -> bool {
        match self {
            ReadCell::Time => true,
            ReadCell::List(element) => element.needs_times(),
            ReadCell::Map(key, value) => key.needs_times() || value.needs_times(),
            ReadCell::Struct(children) => children.iter().any(|(_, child)| child.needs_times()),
            _ => false,
        }
    }

    /// Every value of `array` as a PHP list.
    pub fn values(&self, array: &ArrayRef, times: Option<&Times>) -> Result<Zval, PhpException> {
        let mut list = ZendHashTable::with_capacity(u32::try_from(array.len()).unwrap_or(u32::MAX));

        for row in 0..array.len() {
            list.push(self.value(array.as_ref(), row, times)?).map_err(collected)?;
        }

        let mut zv = Zval::new();
        zv.set_hashtable(list);

        Ok(zv)
    }

    fn value(&self, array: &dyn Array, row: usize, times: Option<&Times>) -> Result<Zval, PhpException> {
        let mut zv = Zval::new();

        if array.is_null(row) {
            return Ok(zv);
        }

        match self {
            ReadCell::Null => {}
            ReadCell::Bool => zv.set_bool(array.as_boolean().value(row)),
            ReadCell::Int => zv = zval_long(array.as_primitive::<Int64Type>().value(row)),
            ReadCell::Float => zv.set_double(array.as_primitive::<Float64Type>().value(row)),
            ReadCell::Bytes => {
                zv = zval_str(match array.data_type() {
                    DataType::FixedSizeBinary(_) => array.as_fixed_size_binary().value(row),
                    _ => array.as_binary::<i32>().value(row),
                });
            }
            ReadCell::Uuid => zv = zval_str(&uuid_text(array.as_fixed_size_binary().value(row))),
            ReadCell::Date => zv = date_from_days(i64::from(array.as_primitive::<Date32Type>().value(row)))?,
            ReadCell::Timestamp => {
                zv = datetime_from_micros(array.as_primitive::<TimestampMicrosecondType>().value(row), b"+00:00")?;
            }
            ReadCell::Time => {
                zv = times
                    .ok_or_else(|| ext_exception("flow_php Parquet reader has no TIME base"))?
                    .interval(array.as_primitive::<DurationMicrosecondType>().value(row))?;
            }
            ReadCell::List(element) => {
                let list = array.as_list::<i32>();
                let offsets = list.value_offsets();
                let mut values = ZendHashTable::with_capacity((offsets[row + 1] - offsets[row]) as u32);

                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    values.push(element.value(list.values().as_ref(), index, times)?).map_err(collected)?;
                }

                zv.set_hashtable(values);
            }
            ReadCell::Map(key, value) => {
                let map = array.as_map();
                let offsets = map.value_offsets();
                let mut entries = ZendHashTable::with_capacity((offsets[row + 1] - offsets[row]) as u32);

                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    let entry = value.value(map.values().as_ref(), index, times)?;

                    match key.value(map.keys().as_ref(), index, times)? {
                        k if k.is_long() => ht_insert_long(&mut entries, k.long().unwrap_or_default(), entry),
                        k => ht_insert(&mut entries, k.zend_str().map_or(&b""[..], |key| key.as_bytes()), entry),
                    }
                }

                zv.set_hashtable(entries);
            }
            ReadCell::Struct(children) => {
                let structure = array.as_struct();
                let mut values = ZendHashTable::with_capacity(children.len() as u32);

                for ((name, child), column) in children.iter().zip(structure.columns()) {
                    ht_insert(&mut values, name, child.value(column.as_ref(), row, times)?);
                }

                zv.set_hashtable(values);
            }
        }

        Ok(zv)
    }
}

/// Child `index` of a nested Parquet-side field.
fn child(parquet: &Field, index: usize) -> Result<&Field, Error> {
    match parquet.data_type() {
        DataType::List(element) | DataType::LargeList(element) | DataType::FixedSizeList(element, _) if index == 0 => {
            Ok(element)
        }
        DataType::Map(entries, _) if index == 0 => Ok(entries),
        DataType::Struct(fields) if index < fields.len() => Ok(&fields[index]),
        other => Err(unsupported(other)),
    }
}

/// `TimeConverter::toDateInterval()`: `$base->diff($base->modify("+{µs} microseconds"))` - `diff()` normalises the
/// duration into d/h and, past a month, m/d exactly as the PHP engine's value. A time of day (< 24 h) normalises into
/// nothing, so it is built directly with `DateInterval::__set_state()`, one call instead of two.
pub struct Times {
    base: Zval,
    modify: &'static Function,
    diff: &'static Function,
    set_state: &'static Function,
}

const DAY: i64 = 86_400_000_000;

impl Times {
    /// `$base = new DateTimeImmutable('1970-01-01 00:00:00.000000', new DateTimeZone('UTC'))`, built once per reader.
    pub fn new() -> Result<Self, PhpException> {
        let ce = find_class("DateTimeImmutable")?;

        Ok(Self {
            base: datetime_from_micros(0, b"UTC")?,
            modify: ce_method_ref(ce, "modify")?,
            diff: ce_method_ref(ce, "diff")?,
            set_state: ce_method_ref(find_class("DateInterval")?, "__set_state")?,
        })
    }

    fn interval(&self, micros: i64) -> Result<Zval, PhpException> {
        if (0..DAY).contains(&micros) {
            return self.time_of_day(micros);
        }

        let base = self.base.object().ok_or_else(|| ext_exception("flow_php expected a TIME base datetime"))?;
        let target = call_handle_transparent(
            self.modify,
            Some(base),
            &mut [zval_str(format!("+{micros} microseconds").as_bytes())],
        )?;

        call_handle_transparent(self.diff, Some(base), &mut [target])
    }

    /// `diff()`'s interval of a time of day: `days` 0, `f` from the whole microseconds.
    fn time_of_day(&self, micros: i64) -> Result<Zval, PhpException> {
        let mut state = ZendHashTable::with_capacity(10);
        let double = |value: f64| {
            let mut zv = Zval::new();
            zv.set_double(value);
            zv
        };

        for (name, value) in [
            ("y", 0),
            ("m", 0),
            ("d", 0),
            ("h", micros / 3_600_000_000),
            ("i", micros / 60_000_000 % 60),
            ("s", micros / 1_000_000 % 60),
        ] {
            ht_insert(&mut state, name.as_bytes(), zval_long(value));
        }

        // __set_state() stores `f` as (int) (f * 1e6): the half microsecond keeps the truncation on the exact value
        // (0.519488 * 1e6 = 519487.99…), and `f` reads back as that value / 1e6, as diff()'s does
        ht_insert(&mut state, b"f", double(((micros % 1_000_000) as f64 + 0.5) / 1_000_000.0));
        ht_insert(&mut state, b"invert", zval_long(0));
        ht_insert(&mut state, b"days", zval_long(0));
        let mut from_string = Zval::new();
        from_string.set_bool(false);
        ht_insert(&mut state, b"from_string", from_string);

        let mut array = Zval::new();
        array.set_hashtable(state);

        call_handle_transparent(self.set_state, None, &mut [array])
    }
}

/// A value `WriteCell::append` refuses.
pub enum Refusal {
    /// What the target type accepts and what it got.
    Value { expected: &'static str, got: String },
    /// A byte buffer past i32 offsets, or a day past i32.
    Overflow,
    /// A PHP exception a call into the value threw.
    Php(PhpException),
}

impl From<PhpException> for Refusal {
    fn from(exception: PhpException) -> Self {
        Refusal::Php(exception)
    }
}

/// `get_debug_type()`.
fn debug_type(value: &Zval) -> String {
    if value.is_null() {
        "null".to_string()
    } else if value.is_bool() {
        "bool".to_string()
    } else if value.is_long() {
        "int".to_string()
    } else if value.is_double() {
        "float".to_string()
    } else if value.is_string() {
        "string".to_string()
    } else if value.is_array() {
        "array".to_string()
    } else if let Some(object) = value.object() {
        object.get_class_name().unwrap_or_else(|_| "object".to_string())
    } else {
        "unknown".to_string()
    }
}

fn refused(expected: &'static str, value: &Zval) -> Refusal {
    Refusal::Value {
        expected,
        got: debug_type(value),
    }
}

/// The classes the write cells test instances of, resolved once per writer.
pub struct Classes {
    datetime: &'static ClassEntry,
    interval: &'static ClassEntry,
}

impl Classes {
    pub fn new() -> Result<Self, PhpException> {
        Ok(Self {
            datetime: find_class("DateTimeInterface")?,
            interval: find_class("DateInterval")?,
        })
    }
}

/// What a writer column accepts per `PhpParquetEngine`'s target type, appended to its canonical kind's builder; the
/// cast to the Parquet type (`to_parquet`) refuses what is out of the target's range.
pub enum WriteCell {
    Bool,
    Int,
    Float,
    /// DECIMAL: a float or an int.
    Decimal,
    Bytes,
    /// UUID: a string, or an object's `toString()` / `__toString()`, whose dash-less text is 32 hex digits.
    Uuid,
    Date,
    Timestamp,
    Time,
    List(Box<WriteCell>, bool),
    Map(Box<WriteCell>, Box<WriteCell>, bool),
    Struct(Vec<(Vec<u8>, WriteCell, bool)>),
}

impl WriteCell {
    /// The cell for a writer schema field (`Flow\Parquet\Engine\Arrow\SchemaConverter::toExtension()`), and the
    /// canonical kind its values are built in.
    pub fn new(target: &Field) -> Result<(Self, Kind), Error> {
        Ok(match target.data_type() {
            DataType::Boolean => (WriteCell::Bool, Kind::Boolean),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => (WriteCell::Int, Kind::Int64),
            DataType::Float32 | DataType::Float64 => (WriteCell::Float, Kind::Float64),
            DataType::Decimal128(_, _) => (WriteCell::Decimal, Kind::Float64),
            DataType::Utf8 | DataType::Binary => (WriteCell::Bytes, Kind::Bytes),
            DataType::FixedSizeBinary(16) if is_uuid(target) => (WriteCell::Uuid, Kind::Uuid),
            DataType::FixedSizeBinary(_) => (WriteCell::Bytes, Kind::Bytes),
            DataType::Date32 => (WriteCell::Date, Kind::Int32),
            DataType::Timestamp(_, _) => (WriteCell::Timestamp, Kind::Timestamp),
            DataType::Time32(_) | DataType::Time64(_) => (WriteCell::Time, Kind::Duration),
            DataType::List(element) => {
                let (cell, kind) = Self::new(element)?;

                (
                    WriteCell::List(Box::new(cell), element.is_nullable()),
                    Kind::List(Box::new(KindField {
                        name: element.name().clone(),
                        kind,
                        optional: element.is_nullable(),
                    })),
                )
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(unsupported(target.data_type()));
                };
                let (key, key_kind) = Self::new(&fields[0])?;
                let (value, value_kind) = Self::new(&fields[1])?;

                (
                    WriteCell::Map(Box::new(key), Box::new(value), fields[1].is_nullable()),
                    Kind::Map(
                        Box::new(KindField {
                            name: fields[0].name().clone(),
                            kind: key_kind,
                            optional: false,
                        }),
                        Box::new(KindField {
                            name: fields[1].name().clone(),
                            kind: value_kind,
                            optional: fields[1].is_nullable(),
                        }),
                    ),
                )
            }
            DataType::Struct(fields) => {
                let mut cells = Vec::with_capacity(fields.len());
                let mut kinds = Vec::with_capacity(fields.len());

                for field in fields {
                    let (cell, kind) = Self::new(field)?;
                    cells.push((field.name().as_bytes().to_vec(), cell, field.is_nullable()));
                    kinds.push(KindField {
                        name: field.name().clone(),
                        kind,
                        optional: field.is_nullable(),
                    });
                }

                (WriteCell::Struct(cells), Kind::Struct(kinds))
            }
            other => return Err(unsupported(other)),
        })
    }

    /// `value` appended to `builder`; a null only where `nullable`.
    pub fn append(&self, builder: &mut KindBuilder, value: &Zval, nullable: bool, classes: &Classes) -> Result<(), Refusal> {
        let value = value.dereference();

        if value.is_null() {
            if !nullable {
                return Err(refused("a value", value));
            }

            builder.append_null();

            return Ok(());
        }

        match self {
            WriteCell::Bool => builder.append_bool(value.bool().ok_or_else(|| refused("bool", value))?),
            WriteCell::Int => builder.append_fixed(&value.long().ok_or_else(|| refused("int", value))?.to_le_bytes()),
            WriteCell::Float => {
                builder.append_fixed(&value.double().ok_or_else(|| refused("float", value))?.to_le_bytes());
            }
            WriteCell::Decimal => {
                let float = match (value.double(), value.long()) {
                    (Some(float), _) => float,
                    (_, Some(int)) => int as f64,
                    _ => return Err(refused("float or int", value)),
                };
                builder.append_fixed(&float.to_le_bytes());
            }
            WriteCell::Bytes => {
                let bytes = value.zend_str().ok_or_else(|| refused("string", value))?;
                builder.append_bytes(bytes.as_bytes()).map_err(|_| Refusal::Overflow)?;
            }
            WriteCell::Uuid => {
                let text = match value.object() {
                    Some(object) => uuid_text_of(object)?.ok_or_else(|| refused("uuid string or object", value))?,
                    None => value.shallow_clone(),
                };
                let bytes = text
                    .zend_str()
                    .and_then(|text| uuid_bytes(text.as_bytes()))
                    .ok_or_else(|| refused("uuid string or object", value))?;
                builder.append_fixed(&bytes);
            }
            WriteCell::Date => {
                let datetime = datetime(value, classes).ok_or_else(|| refused("DateTimeInterface", value))?;
                let days = match datetime_days(datetime) {
                    Some(days) => days,
                    None => fallback_days(datetime)?,
                };
                let days = i32::try_from(days).map_err(|_| Refusal::Overflow)?;
                builder.append_fixed(&days.to_le_bytes());
            }
            WriteCell::Timestamp => {
                let datetime = datetime(value, classes).ok_or_else(|| refused("DateTimeInterface", value))?;
                let micros = match datetime_micros(datetime) {
                    Some(micros) => micros,
                    None => fallback_micros(datetime)?,
                };
                builder.append_fixed(&micros.to_le_bytes());
            }
            WriteCell::Time => {
                let interval = value
                    .object()
                    .filter(|object| object.instance_of(classes.interval))
                    .ok_or_else(|| refused("DateInterval", value))?;
                builder.append_fixed(&interval_micros(interval, value)?.to_le_bytes());
            }
            WriteCell::List(element, element_nullable) => {
                let values = value.array().ok_or_else(|| refused("array", value))?;
                let mut failure = None;

                ht_for_each(values, |_, _, item| {
                    if failure.is_none() {
                        if let Err(refusal) = element.append(builder.list_element(), item, *element_nullable, classes) {
                            failure = Some(refusal);
                        }
                    }

                    Ok(())
                })?;

                if let Some(refusal) = failure {
                    return Err(refusal);
                }

                builder.end_entries().map_err(|_| Refusal::Overflow)?;
            }
            WriteCell::Map(key, map_value, value_nullable) => {
                let entries = value.array().ok_or_else(|| refused("array", value))?;
                let mut failure = None;

                ht_for_each(entries, |name, index, item| {
                    if failure.is_some() {
                        return Ok(());
                    }

                    let entry_key = match name {
                        Some(name) => zval_str(name.as_bytes()),
                        None => zval_long(index as i64),
                    };
                    let (keys, values) = builder.map_entries();
                    let appended = key
                        .append(keys, &entry_key, false, classes)
                        .and_then(|()| map_value.append(values, item, *value_nullable, classes));

                    if let Err(refusal) = appended {
                        failure = Some(refusal);
                    }

                    Ok(())
                })?;

                if let Some(refusal) = failure {
                    return Err(refusal);
                }

                builder.end_entries().map_err(|_| Refusal::Overflow)?;
            }
            WriteCell::Struct(children) => {
                let values = value.array().ok_or_else(|| refused("array", value))?;
                let null = Zval::new();

                for ((name, cell, child_nullable), child) in children.iter().zip(builder.struct_children()) {
                    cell.append(child, ht_get(values, name).unwrap_or(&null), *child_nullable, classes)?;
                }

                builder.end_struct();
            }
        }

        Ok(())
    }
}

/// `UuidConverter::toParquetType()`'s text of an object: `toString()`, else `__toString()`; `None` for neither.
fn uuid_text_of(object: &ZendObject) -> Result<Option<Zval>, PhpException> {
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("flow_php expected a class entry"))?;

    for method in ["toString", "__toString"] {
        if let Ok(function) = ce_method_ref(ce, method) {
            return call_handle_transparent(function, Some(object), &mut []).map(Some);
        }
    }

    Ok(None)
}

fn datetime<'a>(value: &'a Zval, classes: &Classes) -> Option<&'a ZendObject> {
    value.object().filter(|object| object.instance_of(classes.datetime))
}

fn method_long(object: &ZendObject, method: &str, args: &mut [Zval]) -> Result<i64, PhpException> {
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("flow_php expected a class entry"))?;
    let result = call_handle_transparent(ce_method_ref(ce, method)?, Some(object), args)?;

    result
        .long()
        .or_else(|| result.zend_str().and_then(|text| std::str::from_utf8(text.as_bytes()).ok()?.parse().ok()))
        .ok_or_else(|| ext_exception(format!("flow_php expected {method}() to return an int")))
}

/// `getTimestamp() * 1_000_000 + format('u')`, for a datetime timelib has not brought up to date.
fn fallback_micros(datetime: &ZendObject) -> Result<i64, Refusal> {
    let seconds = method_long(datetime, "getTimestamp", &mut [])?;
    let micros = method_long(datetime, "format", &mut [zval_str(b"u")])?;

    seconds.checked_mul(1_000_000).and_then(|us| us.checked_add(micros)).ok_or(Refusal::Overflow)
}

/// `(getTimestamp() + getOffset())` floored to days, for a datetime timelib has not brought up to date.
fn fallback_days(datetime: &ZendObject) -> Result<i64, Refusal> {
    let seconds = method_long(datetime, "getTimestamp", &mut [])?;
    let offset = method_long(datetime, "getOffset", &mut [])?;

    Ok((seconds + offset).div_euclid(86_400))
}

fn interval_part(interval: &ZendObject, name: &str) -> Result<i64, Refusal> {
    Ok(read_property(interval, name)?.long().unwrap_or_default())
}

/// `TimeConverter::toParquetType()`: `((d·24+h)·3600+i·60+s)·1e6 + (int)(f·1e6)`, `invert` ignored; y or m ≠ 0 is
/// refused.
fn interval_micros(interval: &ZendObject, value: &Zval) -> Result<i64, Refusal> {
    if interval_part(interval, "y")? != 0 || interval_part(interval, "m")? != 0 {
        return Err(refused("DateInterval without years or months", value));
    }

    let seconds = ((interval_part(interval, "d")? * 24 + interval_part(interval, "h")?) * 3_600
        + interval_part(interval, "i")? * 60
        + interval_part(interval, "s")?)
        .checked_mul(1_000_000)
        .ok_or(Refusal::Overflow)?;
    let fraction = (read_property(interval, "f")?.double().unwrap_or_default() * 1_000_000.0) as i64;

    seconds.checked_add(fraction).ok_or(Refusal::Overflow)
}

/// Rows appended to one writer column, cast to canonical arrays in batches.
pub struct WriteColumn {
    pub name: String,
    pub cell: WriteCell,
    pub kind: Kind,
    pub nullable: bool,
    pub builder: KindBuilder,
}

impl WriteColumn {
    pub fn new(target: &Field) -> Result<Self, Error> {
        let (cell, kind) = WriteCell::new(target).map_err(|e| e.in_column(target.name()))?;

        Ok(Self {
            name: target.name().clone(),
            builder: KindBuilder::new(&kind),
            cell,
            kind,
            nullable: target.is_nullable(),
        })
    }

    /// The rows appended since the last take, as a canonical array; the builder starts over.
    pub fn take(&mut self) -> Result<ArrayRef, Error> {
        let data = self.builder.finish(&self.kind)?;
        self.builder = KindBuilder::new(&self.kind);

        Ok(make_array(data))
    }
}
