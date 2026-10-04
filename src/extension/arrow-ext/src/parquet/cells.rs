//! The `Flow\Parquet` lib's per-cell values over the core's canonical arrays: what `PhpParquetEngine` returns for a
//! Parquet value on read, and what it accepts per target type on write.

use std::any::Any;
use std::sync::Arc;

use arrow_array::builder::{
    ArrayBuilder, BinaryBuilder, BooleanBuilder, Date32Builder, DurationMicrosecondBuilder, FixedSizeBinaryBuilder,
    Float64Builder, Int64Builder, ListBuilder, MapBuilder, MapFieldNames, TimestampMicrosecondBuilder,
};
use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::{Array, ArrayRef, StructArray};
use arrow_buffer::NullBufferBuilder;
use arrow_schema::{DataType, Field, Fields, TimeUnit};
use ext_php_rs::exception::PhpException;
use ext_php_rs::types::{ZendHashTable, ZendObject, Zval};
use ext_php_rs::zend::{ClassEntry, Function};

use crate::exception::ext_exception;
use crate::parquet::error::Error;
use crate::php::{
    call_handle_transparent, ce_method_ref, find_class, ht_for_each, ht_get, ht_insert, ht_insert_long, read_property,
    zval_long, zval_str,
};
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
    ext_exception(format!("arrow failed to collect a Parquet value: {error:?}"))
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
                        Ok((
                            field.name().as_bytes().to_vec(),
                            Self::new(field.data_type(), child(parquet, index)?)?,
                        ))
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
                    .ok_or_else(|| ext_exception("arrow Parquet reader has no TIME base"))?
                    .interval(array.as_primitive::<DurationMicrosecondType>().value(row))?;
            }
            ReadCell::List(element) => {
                let list = array.as_list::<i32>();
                let offsets = list.value_offsets();
                let mut values = ZendHashTable::with_capacity((offsets[row + 1] - offsets[row]) as u32);

                for index in offsets[row] as usize..offsets[row + 1] as usize {
                    values
                        .push(element.value(list.values().as_ref(), index, times)?)
                        .map_err(collected)?;
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

        let base = self
            .base
            .object()
            .ok_or_else(|| ext_exception("arrow expected a TIME base datetime"))?;
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
        ht_insert(
            &mut state,
            b"f",
            double(((micros % 1_000_000) as f64 + 0.5) / 1_000_000.0),
        );
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

/// A value `WriteCell::check` refuses.
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

/// A value a scalar cell accepted. `Copy`: a flat writer's row buffer clears without dropping anything.
#[derive(Clone, Copy)]
pub enum Scalar<'a> {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(&'a [u8]),
    Uuid([u8; 16]),
    Date(i32),
    Micros(i64),
}

impl Scalar<'_> {
    /// The same value with its bytes, if any, read from `bytes`: a map key checked from a key zval built for the
    /// check, whose bytes are the array key's.
    fn rebound(self, bytes: &[u8]) -> Scalar<'_> {
        match self {
            Scalar::Null => Scalar::Null,
            Scalar::Bool(value) => Scalar::Bool(value),
            Scalar::Int(value) => Scalar::Int(value),
            Scalar::Float(value) => Scalar::Float(value),
            Scalar::Bytes(_) => Scalar::Bytes(bytes),
            Scalar::Uuid(value) => Scalar::Uuid(value),
            Scalar::Date(days) => Scalar::Date(days),
            Scalar::Micros(micros) => Scalar::Micros(micros),
        }
    }
}

fn missing<'a>(nullable: bool) -> Result<Scalar<'a>, Refusal> {
    match nullable {
        true => Ok(Scalar::Null),
        false => Err(Refusal::Value {
            expected: "a value",
            got: "null".to_string(),
        }),
    }
}

/// A value a cell accepted, as its canonical builder appends it.
pub enum Validated<'a> {
    Scalar(Scalar<'a>),
    List(Vec<Validated<'a>>),
    Map(Vec<(Validated<'a>, Validated<'a>)>),
    Struct(Vec<Validated<'a>>),
}

/// What a writer buffers per cell of a row until the whole row is checked: `Scalar` when every writer column is
/// scalar, `Validated` otherwise.
pub trait Checked<'a>: Sized {
    fn check(cell: &WriteCell, value: &'a Zval, nullable: bool, classes: &Classes) -> Result<Self, Refusal>;

    /// A writer column the row lacks: null, refused where not `nullable`.
    fn missing(nullable: bool) -> Result<Self, Refusal>;

    /// Whether `builder` takes the value with its offsets in i32.
    fn fits(&self, cell: &WriteCell, builder: &mut ColumnBuilder) -> bool;

    fn append(&self, cell: &WriteCell, builder: &mut ColumnBuilder);
}

impl<'a> Checked<'a> for Scalar<'a> {
    fn check(cell: &WriteCell, value: &'a Zval, nullable: bool, classes: &Classes) -> Result<Self, Refusal> {
        cell.check_scalar(value, nullable, classes)
    }

    fn missing(nullable: bool) -> Result<Self, Refusal> {
        missing(nullable)
    }

    fn fits(&self, _: &WriteCell, builder: &mut ColumnBuilder) -> bool {
        match (self, builder) {
            (Scalar::Bytes(bytes), ColumnBuilder::Bytes(builder)) => {
                offsets_fit(builder.values_slice().len(), bytes.len())
            }
            _ => true,
        }
    }

    fn append(&self, cell: &WriteCell, builder: &mut ColumnBuilder) {
        cell.append_scalar(builder, self);
    }
}

impl<'a> Checked<'a> for Validated<'a> {
    fn check(cell: &WriteCell, value: &'a Zval, nullable: bool, classes: &Classes) -> Result<Self, Refusal> {
        cell.check(value, nullable, classes)
    }

    fn missing(nullable: bool) -> Result<Self, Refusal> {
        missing(nullable).map(Validated::Scalar)
    }

    fn fits(&self, cell: &WriteCell, builder: &mut ColumnBuilder) -> bool {
        cell.fits(builder, &[self])
    }

    fn append(&self, cell: &WriteCell, builder: &mut ColumnBuilder) {
        cell.append(builder, self);
    }
}

/// The builder of a writer cell's canonical type, resolved once per column: appends match it with the cell, nothing
/// is downcast per value.
pub enum ColumnBuilder {
    Bool(BooleanBuilder),
    Int(Int64Builder),
    Float(Float64Builder),
    Bytes(BinaryBuilder),
    Uuid(FixedSizeBinaryBuilder),
    Date(Date32Builder),
    Timestamp(TimestampMicrosecondBuilder),
    Time(DurationMicrosecondBuilder),
    List(Box<ListBuilder<ColumnBuilder>>),
    Map(Box<MapBuilder<ColumnBuilder, ColumnBuilder>>),
    Struct(Box<StructColumnBuilder>),
}

pub struct StructColumnBuilder {
    fields: Fields,
    children: Vec<ColumnBuilder>,
    nulls: NullBufferBuilder,
}

impl StructColumnBuilder {
    fn finish(&mut self) -> ArrayRef {
        let length = self.nulls.len();
        let children = self.children.iter_mut().map(ArrayBuilder::finish).collect();

        Arc::new(
            StructArray::try_new_with_length(self.fields.clone(), children, self.nulls.finish(), length)
                .expect("every child appends a slot per struct slot"),
        )
    }

    fn finish_cloned(&self) -> ArrayRef {
        let children = self.children.iter().map(ArrayBuilder::finish_cloned).collect();

        Arc::new(
            StructArray::try_new_with_length(
                self.fields.clone(),
                children,
                self.nulls.finish_cloned(),
                self.nulls.len(),
            )
            .expect("every child appends a slot per struct slot"),
        )
    }
}

impl ColumnBuilder {
    /// `canonical`: what `WriteCell::new()` returned for the cell.
    pub fn new(canonical: &DataType, capacity: usize) -> Self {
        match canonical {
            DataType::Boolean => ColumnBuilder::Bool(BooleanBuilder::with_capacity(capacity)),
            DataType::Int64 => ColumnBuilder::Int(Int64Builder::with_capacity(capacity)),
            DataType::Float64 => ColumnBuilder::Float(Float64Builder::with_capacity(capacity)),
            DataType::Binary => ColumnBuilder::Bytes(BinaryBuilder::with_capacity(capacity, 1024)),
            DataType::FixedSizeBinary(size) => {
                ColumnBuilder::Uuid(FixedSizeBinaryBuilder::with_capacity(capacity, *size))
            }
            DataType::Date32 => ColumnBuilder::Date(Date32Builder::with_capacity(capacity)),
            DataType::Timestamp(_, _) => ColumnBuilder::Timestamp(
                TimestampMicrosecondBuilder::with_capacity(capacity).with_data_type(canonical.clone()),
            ),
            DataType::Duration(_) => ColumnBuilder::Time(DurationMicrosecondBuilder::with_capacity(capacity)),
            DataType::List(element) => ColumnBuilder::List(Box::new(
                ListBuilder::with_capacity(Self::new(element.data_type(), capacity), capacity)
                    .with_field(Arc::clone(element)),
            )),
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    unreachable!("WriteCell::new() builds map entries as a struct")
                };

                ColumnBuilder::Map(Box::new(
                    MapBuilder::with_capacity(
                        Some(MapFieldNames {
                            entry: entries.name().clone(),
                            key: fields[0].name().clone(),
                            value: fields[1].name().clone(),
                        }),
                        Self::new(fields[0].data_type(), capacity),
                        Self::new(fields[1].data_type(), capacity),
                        capacity,
                    )
                    .with_keys_field(Arc::clone(&fields[0]))
                    .with_values_field(Arc::clone(&fields[1])),
                ))
            }
            DataType::Struct(fields) => ColumnBuilder::Struct(Box::new(StructColumnBuilder {
                fields: fields.clone(),
                children: fields
                    .iter()
                    .map(|field| Self::new(field.data_type(), capacity))
                    .collect(),
                nulls: NullBufferBuilder::new(capacity),
            })),
            other => unreachable!("WriteCell::new() builds no {other} column"),
        }
    }
}

impl ArrayBuilder for ColumnBuilder {
    fn len(&self) -> usize {
        match self {
            ColumnBuilder::Bool(builder) => builder.len(),
            ColumnBuilder::Int(builder) => builder.len(),
            ColumnBuilder::Float(builder) => builder.len(),
            ColumnBuilder::Bytes(builder) => builder.len(),
            ColumnBuilder::Uuid(builder) => builder.len(),
            ColumnBuilder::Date(builder) => builder.len(),
            ColumnBuilder::Timestamp(builder) => builder.len(),
            ColumnBuilder::Time(builder) => builder.len(),
            ColumnBuilder::List(builder) => builder.len(),
            ColumnBuilder::Map(builder) => builder.len(),
            ColumnBuilder::Struct(builder) => builder.nulls.len(),
        }
    }

    fn finish(&mut self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Int(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Float(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Bytes(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Uuid(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Date(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Timestamp(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Time(builder) => Arc::new(builder.finish()),
            ColumnBuilder::List(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Map(builder) => Arc::new(builder.finish()),
            ColumnBuilder::Struct(builder) => builder.finish(),
        }
    }

    fn finish_cloned(&self) -> ArrayRef {
        match self {
            ColumnBuilder::Bool(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Int(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Float(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Bytes(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Uuid(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Date(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Timestamp(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Time(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::List(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Map(builder) => Arc::new(builder.finish_cloned()),
            ColumnBuilder::Struct(builder) => builder.finish_cloned(),
        }
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    fn as_any_mut(&mut self) -> &mut dyn Any {
        self
    }

    fn into_box_any(self: Box<Self>) -> Box<dyn Any> {
        self
    }
}

/// Offsets stay i32: `current` entries plus `added` must not pass `i32::MAX`.
fn offsets_fit(current: usize, added: usize) -> bool {
    current
        .checked_add(added)
        .is_some_and(|total| total <= i32::MAX as usize)
}

/// What a writer column accepts per `PhpParquetEngine`'s target type, appended to its canonical type's builder; the
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
    /// canonical type its values are built in.
    pub fn new(target: &Field) -> Result<(Self, DataType), Error> {
        Ok(match target.data_type() {
            DataType::Boolean => (WriteCell::Bool, DataType::Boolean),
            DataType::Int8
            | DataType::Int16
            | DataType::Int32
            | DataType::Int64
            | DataType::UInt8
            | DataType::UInt16
            | DataType::UInt32
            | DataType::UInt64 => (WriteCell::Int, DataType::Int64),
            DataType::Float32 | DataType::Float64 => (WriteCell::Float, DataType::Float64),
            DataType::Decimal128(_, _) => (WriteCell::Decimal, DataType::Float64),
            DataType::Utf8 | DataType::Binary => (WriteCell::Bytes, DataType::Binary),
            DataType::FixedSizeBinary(16) if is_uuid(target) => (WriteCell::Uuid, DataType::FixedSizeBinary(16)),
            DataType::FixedSizeBinary(_) => (WriteCell::Bytes, DataType::Binary),
            DataType::Date32 => (WriteCell::Date, DataType::Date32),
            DataType::Timestamp(_, _) => (
                WriteCell::Timestamp,
                DataType::Timestamp(TimeUnit::Microsecond, Some("UTC".into())),
            ),
            DataType::Time32(_) | DataType::Time64(_) => (WriteCell::Time, DataType::Duration(TimeUnit::Microsecond)),
            DataType::List(element) => {
                let (cell, canonical) = Self::new(element)?;

                (
                    WriteCell::List(Box::new(cell), element.is_nullable()),
                    DataType::List(Arc::new(Field::new("item", canonical, true))),
                )
            }
            DataType::Map(entries, _) => {
                let DataType::Struct(fields) = entries.data_type() else {
                    return Err(unsupported(target.data_type()));
                };
                let (key, key_type) = Self::new(&fields[0])?;
                let (value, value_type) = Self::new(&fields[1])?;

                (
                    WriteCell::Map(Box::new(key), Box::new(value), fields[1].is_nullable()),
                    DataType::Map(
                        Arc::new(Field::new(
                            "entries",
                            DataType::Struct(Fields::from(vec![
                                Field::new("key", key_type, false),
                                Field::new("value", value_type, true),
                            ])),
                            false,
                        )),
                        false,
                    ),
                )
            }
            DataType::Struct(fields) => {
                let mut cells = Vec::with_capacity(fields.len());
                let mut children = Vec::with_capacity(fields.len());

                for field in fields {
                    let (cell, canonical) = Self::new(field)?;
                    cells.push((field.name().as_bytes().to_vec(), cell, field.is_nullable()));
                    children.push(Field::new(field.name(), canonical, true));
                }

                (WriteCell::Struct(cells), DataType::Struct(Fields::from(children)))
            }
            other => return Err(unsupported(other)),
        })
    }

    /// `value` as this scalar cell accepts it, a null only where `nullable`; a nested cell refuses it as not an
    /// array.
    pub fn check_scalar<'a>(&self, value: &'a Zval, nullable: bool, classes: &Classes) -> Result<Scalar<'a>, Refusal> {
        let value = value.dereference();

        if value.is_null() {
            return if nullable {
                Ok(Scalar::Null)
            } else {
                Err(refused("a value", value))
            };
        }

        Ok(match self {
            WriteCell::Bool => Scalar::Bool(value.bool().ok_or_else(|| refused("bool", value))?),
            WriteCell::Int => Scalar::Int(value.long().ok_or_else(|| refused("int", value))?),
            WriteCell::Float => Scalar::Float(value.double().ok_or_else(|| refused("float", value))?),
            WriteCell::Decimal => Scalar::Float(match (value.double(), value.long()) {
                (Some(float), _) => float,
                (_, Some(int)) => int as f64,
                _ => return Err(refused("float or int", value)),
            }),
            WriteCell::Bytes => Scalar::Bytes(value.zend_str().ok_or_else(|| refused("string", value))?.as_bytes()),
            WriteCell::Uuid => {
                let text = match value.object() {
                    Some(object) => uuid_text_of(object)?.ok_or_else(|| refused("uuid string or object", value))?,
                    None => value.shallow_clone(),
                };

                Scalar::Uuid(
                    text.zend_str()
                        .and_then(|text| uuid_bytes(text.as_bytes()))
                        .ok_or_else(|| refused("uuid string or object", value))?,
                )
            }
            WriteCell::Date => {
                let datetime = datetime(value, classes).ok_or_else(|| refused("DateTimeInterface", value))?;
                let days = match datetime_days(datetime) {
                    Some(days) => days,
                    None => fallback_days(datetime)?,
                };

                Scalar::Date(i32::try_from(days).map_err(|_| Refusal::Overflow)?)
            }
            WriteCell::Timestamp => {
                let datetime = datetime(value, classes).ok_or_else(|| refused("DateTimeInterface", value))?;

                Scalar::Micros(match datetime_micros(datetime) {
                    Some(micros) => micros,
                    None => fallback_micros(datetime)?,
                })
            }
            WriteCell::Time => {
                let interval = value
                    .object()
                    .filter(|object| object.instance_of(classes.interval))
                    .ok_or_else(|| refused("DateInterval", value))?;

                Scalar::Micros(interval_micros(interval, value)?)
            }
            WriteCell::List(_, _) | WriteCell::Map(_, _, _) | WriteCell::Struct(_) => {
                return Err(refused("array", value))
            }
        })
    }

    /// `value` as this cell accepts it; a null only where `nullable`.
    pub fn check<'a>(&self, value: &'a Zval, nullable: bool, classes: &Classes) -> Result<Validated<'a>, Refusal> {
        let value = value.dereference();

        if value.is_null() {
            return if nullable {
                Ok(Validated::Scalar(Scalar::Null))
            } else {
                Err(refused("a value", value))
            };
        }

        Ok(match self {
            WriteCell::List(element, element_nullable) => {
                let items = value.array().ok_or_else(|| refused("array", value))?;
                let mut values = Vec::with_capacity(items.len());
                let mut failure = None;

                ht_for_each(items, |_, _, item| {
                    if failure.is_none() {
                        match element.check(item, *element_nullable, classes) {
                            Ok(validated) => values.push(validated),
                            Err(refusal) => failure = Some(refusal),
                        }
                    }

                    Ok(())
                })?;

                if let Some(refusal) = failure {
                    return Err(refusal);
                }

                Validated::List(values)
            }
            WriteCell::Map(key, map_value, value_nullable) => {
                let items = value.array().ok_or_else(|| refused("array", value))?;
                let mut entries = Vec::with_capacity(items.len());
                let mut failure = None;

                ht_for_each(items, |name, index, item| {
                    if failure.is_some() {
                        return Ok(());
                    }

                    let entry_key = match name {
                        Some(name) => zval_str(name.as_bytes()),
                        None => zval_long(index as i64),
                    };
                    let checked = key
                        .check_scalar(&entry_key, false, classes)
                        .map(|key| Validated::Scalar(key.rebound(name.map_or(&[][..], |name| name.as_bytes()))))
                        .and_then(|key| Ok((key, map_value.check(item, *value_nullable, classes)?)));

                    match checked {
                        Ok(entry) => entries.push(entry),
                        Err(refusal) => failure = Some(refusal),
                    }

                    Ok(())
                })?;

                if let Some(refusal) = failure {
                    return Err(refusal);
                }

                Validated::Map(entries)
            }
            WriteCell::Struct(children) => {
                let values = value.array().ok_or_else(|| refused("array", value))?;
                let mut checked = Vec::with_capacity(children.len());

                for (name, cell, child_nullable) in children {
                    checked.push(match ht_get(values, name) {
                        Some(child) => cell.check(child, *child_nullable, classes)?,
                        None => Validated::Scalar(missing(*child_nullable)?),
                    });
                }

                Validated::Struct(checked)
            }
            scalar => Validated::Scalar(scalar.check_scalar(value, nullable, classes)?),
        })
    }

    /// Whether `builder` takes `values` (one row's, every value bound for it) with its offsets in i32; only cells
    /// holding offsets can pass it.
    pub fn fits(&self, builder: &mut ColumnBuilder, values: &[&Validated]) -> bool {
        match (self, builder) {
            (WriteCell::Bytes, ColumnBuilder::Bytes(bytes)) => offsets_fit(
                bytes.values_slice().len(),
                values
                    .iter()
                    .map(|value| match value {
                        Validated::Scalar(Scalar::Bytes(bytes)) => bytes.len(),
                        _ => 0,
                    })
                    .sum(),
            ),
            (WriteCell::List(element, _), ColumnBuilder::List(list)) => {
                let items = values
                    .iter()
                    .flat_map(|value| match value {
                        Validated::List(items) => items.as_slice(),
                        _ => &[],
                    })
                    .collect::<Vec<_>>();

                offsets_fit(list.values().len(), items.len()) && element.fits(list.values(), &items)
            }
            (WriteCell::Map(key, value, _), ColumnBuilder::Map(map)) => {
                let entries = values
                    .iter()
                    .flat_map(|value| match value {
                        Validated::Map(entries) => entries.as_slice(),
                        _ => &[],
                    })
                    .collect::<Vec<_>>();
                let keys = entries.iter().map(|(key, _)| key).collect::<Vec<_>>();
                let items = entries.iter().map(|(_, value)| value).collect::<Vec<_>>();
                let (key_builder, value_builder) = map.entries();

                offsets_fit(key_builder.len(), entries.len())
                    && key.fits(key_builder, &keys)
                    && value.fits(value_builder, &items)
            }
            (WriteCell::Struct(children), ColumnBuilder::Struct(structure)) => children
                .iter()
                .zip(&mut structure.children)
                .enumerate()
                .all(|(index, ((_, cell, _), child))| {
                    let values = values
                        .iter()
                        .filter_map(|value| match value {
                            Validated::Struct(children) => Some(&children[index]),
                            _ => None,
                        })
                        .collect::<Vec<_>>();

                    cell.fits(child, &values)
                }),
            _ => true,
        }
    }

    /// `value`, checked by this cell and fit by [`WriteCell::fits`], appended to `builder`.
    pub fn append(&self, builder: &mut ColumnBuilder, value: &Validated) {
        match (self, builder, value) {
            (_, builder, Validated::Scalar(scalar)) => self.append_scalar(builder, scalar),
            (WriteCell::List(element, _), ColumnBuilder::List(list), Validated::List(items)) => {
                for item in items {
                    element.append(list.values(), item);
                }

                list.append(true);
            }
            (WriteCell::Map(key, map_value, _), ColumnBuilder::Map(map), Validated::Map(entries)) => {
                for (entry_key, entry_value) in entries {
                    let (keys, values) = map.entries();
                    key.append(keys, entry_key);
                    map_value.append(values, entry_value);
                }

                map.append(true).expect("every entry appends a key and a value");
            }
            (WriteCell::Struct(children), ColumnBuilder::Struct(structure), Validated::Struct(values)) => {
                for (((_, cell, _), child), value) in children.iter().zip(&mut structure.children).zip(values) {
                    cell.append(child, value);
                }

                structure.nulls.append_non_null();
            }
            _ => unreachable!("a cell appends only what it checked, to the builder of its canonical type"),
        }
    }

    /// `scalar`, checked by this cell, appended to `builder`.
    pub fn append_scalar(&self, builder: &mut ColumnBuilder, scalar: &Scalar) {
        match (self, builder, scalar) {
            (_, builder, Scalar::Null) => self.append_null(builder),
            (WriteCell::Bool, ColumnBuilder::Bool(builder), Scalar::Bool(value)) => builder.append_value(*value),
            (WriteCell::Int, ColumnBuilder::Int(builder), Scalar::Int(value)) => builder.append_value(*value),
            (WriteCell::Float | WriteCell::Decimal, ColumnBuilder::Float(builder), Scalar::Float(value)) => {
                builder.append_value(*value);
            }
            (WriteCell::Bytes, ColumnBuilder::Bytes(builder), Scalar::Bytes(bytes)) => builder.append_value(bytes),
            (WriteCell::Uuid, ColumnBuilder::Uuid(builder), Scalar::Uuid(bytes)) => {
                builder.append_value(bytes).expect("a uuid is 16 bytes");
            }
            (WriteCell::Date, ColumnBuilder::Date(builder), Scalar::Date(days)) => builder.append_value(*days),
            (WriteCell::Timestamp, ColumnBuilder::Timestamp(builder), Scalar::Micros(micros)) => {
                builder.append_value(*micros);
            }
            (WriteCell::Time, ColumnBuilder::Time(builder), Scalar::Micros(micros)) => builder.append_value(*micros),
            _ => unreachable!("a cell appends only what it checked, to the builder of its canonical type"),
        }
    }

    /// Whether this cell holds one value per row: no list, map or struct.
    pub fn is_scalar(&self) -> bool {
        !matches!(
            self,
            WriteCell::List(_, _) | WriteCell::Map(_, _, _) | WriteCell::Struct(_)
        )
    }

    /// A null slot; a struct's children get one too.
    fn append_null(&self, builder: &mut ColumnBuilder) {
        match (self, builder) {
            (WriteCell::Struct(children), ColumnBuilder::Struct(structure)) => {
                for ((_, cell, _), child) in children.iter().zip(&mut structure.children) {
                    cell.append_null(child);
                }

                structure.nulls.append_null();
            }
            (_, ColumnBuilder::Bool(builder)) => builder.append_null(),
            (_, ColumnBuilder::Int(builder)) => builder.append_null(),
            (_, ColumnBuilder::Float(builder)) => builder.append_null(),
            (_, ColumnBuilder::Bytes(builder)) => builder.append_null(),
            (_, ColumnBuilder::Uuid(builder)) => builder.append_null(),
            (_, ColumnBuilder::Date(builder)) => builder.append_null(),
            (_, ColumnBuilder::Timestamp(builder)) => builder.append_null(),
            (_, ColumnBuilder::Time(builder)) => builder.append_null(),
            (_, ColumnBuilder::List(builder)) => builder.append_null(),
            (_, ColumnBuilder::Map(builder)) => builder.append(false).expect("a null map appends no entries"),
            (_, ColumnBuilder::Struct(_)) => {
                unreachable!("only a struct cell builds a struct column")
            }
        }
    }
}

/// `UuidConverter::toParquetType()`'s text of an object: `toString()`, else `__toString()`; `None` for neither.
fn uuid_text_of(object: &ZendObject) -> Result<Option<Zval>, PhpException> {
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("arrow expected a class entry"))?;

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
    let ce = unsafe { object.ce.as_ref() }.ok_or_else(|| ext_exception("arrow expected a class entry"))?;
    let result = call_handle_transparent(ce_method_ref(ce, method)?, Some(object), args)?;

    result
        .long()
        .or_else(|| {
            result
                .zend_str()
                .and_then(|text| std::str::from_utf8(text.as_bytes()).ok()?.parse().ok())
        })
        .ok_or_else(|| ext_exception(format!("arrow expected {method}() to return an int")))
}

/// `getTimestamp() * 1_000_000 + format('u')`, for a datetime timelib has not brought up to date.
fn fallback_micros(datetime: &ZendObject) -> Result<i64, Refusal> {
    let seconds = method_long(datetime, "getTimestamp", &mut [])?;
    let micros = method_long(datetime, "format", &mut [zval_str(b"u")])?;

    seconds
        .checked_mul(1_000_000)
        .and_then(|us| us.checked_add(micros))
        .ok_or(Refusal::Overflow)
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

/// Rows appended to one writer column, finished into canonical arrays in batches.
pub struct WriteColumn {
    pub name: String,
    pub cell: WriteCell,
    pub nullable: bool,
    pub builder: ColumnBuilder,
}

impl WriteColumn {
    pub fn new(target: &Field, capacity: usize) -> Result<Self, Error> {
        let (cell, canonical) = WriteCell::new(target).map_err(|e| e.in_column(target.name()))?;

        Ok(Self {
            name: target.name().clone(),
            builder: ColumnBuilder::new(&canonical, capacity),
            cell,
            nullable: target.is_nullable(),
        })
    }

    /// The rows appended since the last take, as a canonical array; the builder starts over.
    pub fn take(&mut self) -> ArrayRef {
        self.builder.finish()
    }
}

#[cfg(test)]
mod tests {
    use super::offsets_fit;

    #[test]
    fn offsets_fit_up_to_i32_max() {
        assert!(offsets_fit(i32::MAX as usize - 10, 10));
        assert!(!offsets_fit(i32::MAX as usize - 10, 11));
        assert!(!offsets_fit(usize::MAX, 1));
    }
}
