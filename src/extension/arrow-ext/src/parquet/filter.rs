//! The row filter arrow-rs's push decoder runs: a row is kept only when TRUE; NULL is TRUE in neither polarity.

use std::cmp::Ordering;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, Float32Type, Float64Type, Int16Type, Int32Type, Int64Type,
    Int8Type, Time32MillisecondType, Time32SecondType, Time64MicrosecondType, Time64NanosecondType,
    TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType, TimestampSecondType, UInt16Type,
    UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{Array, ArrayRef, ArrowPrimitiveType, BooleanArray, RecordBatch};
use arrow_buffer::{i256, BooleanBuffer};
use arrow_schema::{ArrowError, DataType, Schema, TimeUnit};
use parquet::arrow::arrow_reader::ArrowPredicate;
use parquet::arrow::ProjectionMask;
use parquet::basic::{ConvertedType, LogicalType, Type};
use parquet::schema::types::{ColumnDescriptor, SchemaDescriptor};

use crate::parquet::error::Error;
use crate::parquet::read::{struct_child, struct_field};

/// A PHP value as the converter's array holds it.
#[derive(Debug, Clone, PartialEq)]
pub enum Raw {
    Null,
    Bool(bool),
    Int(i64),
    Float(f64),
    Bytes(Vec<u8>),
    List(Vec<Raw>),
    Map(Vec<(Vec<u8>, Raw)>),
}

/// A physical bound as PHP sends it: INT32/INT64 values (the UINT_64 bit pattern included), floats, booleans, bytes
/// (BYTE_ARRAY, FIXED_LEN_BYTE_ARRAY, DECIMAL unscaled big-endian two's complement).
#[derive(Debug, Clone, PartialEq)]
enum Scalar {
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// A null bound is unbounded.
#[derive(Debug, Clone, PartialEq)]
struct Interval<T> {
    lower: Option<T>,
    lower_inclusive: bool,
    upper: Option<T>,
    upper_inclusive: bool,
}

/// Per row of a leaf, whether its value matches; a null row's slot holds no value and is masked by its caller.
type Matcher = Box<dyn Fn(&ArrayRef) -> BooleanBuffer + Send>;

/// The `ArrowPredicate` of one root conjunct, checked against the file's arrow schema when built.
pub struct PlanPredicate {
    leaves: Vec<usize>,
    mask: ProjectionMask,
    root: Compiled,
}

impl PlanPredicate {
    /// One predicate per root conjunct of `filter`, in order: arrow-rs evaluates each on the rows the ones before kept.
    pub fn conjuncts(filter: &Raw, schema: &Schema, descr: &SchemaDescriptor) -> Result<Vec<Self>, Error> {
        conjuncts(filter)?
            .into_iter()
            .map(|conjunct| {
                let mut leaves = Vec::new();
                let root = Compiled::parse(conjunct, schema, descr, &mut leaves)?;

                Ok(Self {
                    mask: ProjectionMask::leaves(descr, leaves.iter().copied()),
                    leaves,
                    root,
                })
            })
            .collect()
    }

    /// The parquet leaf of each path the conjunct reads.
    pub fn leaves(&self) -> &[usize] {
        &self.leaves
    }
}

impl ArrowPredicate for PlanPredicate {
    fn projection(&self) -> &ProjectionMask {
        &self.mask
    }

    fn evaluate(&mut self, batch: RecordBatch) -> Result<BooleanArray, ArrowError> {
        Ok(BooleanArray::new(self.root.truth(&batch, false)?, None))
    }
}

/// A filter node with each leaf's match typed for the arrow array the leaf is read as.
enum Compiled {
    And(Vec<Compiled>),
    Or(Vec<Compiled>),
    Not(Box<Compiled>),
    Matches {
        path: Vec<String>,
        matcher: Matcher,
    },
    IsNull {
        path: Vec<String>,
    },
    /// A leaf on a column absent from the file, null in every row: TRUE for an IS NULL, NULL for any other leaf.
    Absent {
        is_null: bool,
    },
}

impl Compiled {
    /// `leaves` gains the parquet leaf of each path the node reads, once.
    fn parse(raw: &Raw, schema: &Schema, descr: &SchemaDescriptor, leaves: &mut Vec<usize>) -> Result<Self, Error> {
        let entries = map(raw, "a filter node")?;
        let mut children = |raw: &Raw, what: &str| {
            list(raw, what)?
                .iter()
                .map(|child| Self::parse(child, schema, descr, leaves))
                .collect::<Result<Vec<_>, _>>()
        };

        if let [(key, value)] = entries {
            match key.as_slice() {
                b"and" => return Ok(Self::And(children(value, "'and'")?)),
                b"or" => return Ok(Self::Or(children(value, "'or'")?)),
                b"not" => return Ok(Self::Not(Box::new(Self::parse(value, schema, descr, leaves)?))),
                _ => {}
            }
        }

        Ok(match field(entries, b"leaf")? {
            Raw::Bytes(kind) if kind == b"intervals" => {
                let path = path(field(entries, b"path")?)?;
                let intervals = list(field(entries, b"intervals")?, "'intervals'")?
                    .iter()
                    .map(interval)
                    .collect::<Result<Vec<_>, _>>()?;
                let column = descr.column(leaf(descr, &path, leaves)?);

                Self::Matches {
                    matcher: bounds(&column, &path, &intervals, &data_type(schema, &path)?)?,
                    path,
                }
            }
            Raw::Bytes(kind) if kind == b"is_null" => {
                let path = path(field(entries, b"path")?)?;
                leaf(descr, &path, leaves)?;
                data_type(schema, &path)?;

                Self::IsNull { path }
            }
            Raw::Bytes(kind) if kind == b"starts_with" => {
                let path = path(field(entries, b"path")?)?;
                let prefix = match field(entries, b"prefix")? {
                    Raw::Bytes(prefix) => prefix.clone(),
                    other => return Err(malformed(format!("a prefix must be a string, got {other:?}"))),
                };
                leaf(descr, &path, leaves)?;
                let data_type = data_type(schema, &path)?;

                Self::Matches {
                    matcher: by_bytes(
                        ByteArray::of(&data_type).ok_or_else(|| unsupported(&path, &data_type))?,
                        move |value| value.starts_with(&prefix),
                    ),
                    path,
                }
            }
            Raw::Bytes(kind) if kind == b"absent" => Self::Absent {
                is_null: match field(entries, b"is_null")? {
                    Raw::Bool(is_null) => *is_null,
                    other => return Err(malformed(format!("'is_null' must be a bool, got {other:?}"))),
                },
            },
            leaf => return Err(malformed(format!("unknown leaf {leaf:?}"))),
        })
    }

    /// The rows the node is TRUE for: under `negated`, the rows its negation is TRUE for.
    fn truth(&self, batch: &RecordBatch, negated: bool) -> Result<BooleanBuffer, ArrowError> {
        let rows = batch.num_rows();

        Ok(match self {
            Self::And(children) | Self::Or(children) => {
                let conjunction = matches!(self, Self::And(_)) != negated;
                let mut result = if conjunction {
                    BooleanBuffer::new_set(rows)
                } else {
                    BooleanBuffer::new_unset(rows)
                };

                for child in children {
                    let truth = child.truth(batch, negated)?;
                    result = if conjunction {
                        &result & &truth
                    } else {
                        &result | &truth
                    };
                }

                result
            }
            Self::Not(child) => child.truth(batch, !negated)?,
            Self::Absent { is_null } if *is_null && !negated => BooleanBuffer::new_set(rows),
            Self::Absent { .. } => BooleanBuffer::new_unset(rows),
            Self::IsNull { path } => {
                let (_, valid) = column(batch, path)?;

                if negated {
                    valid
                } else {
                    !&valid
                }
            }
            Self::Matches { path, matcher } => {
                let (array, valid) = column(batch, path)?;
                let matches = matcher(&array);

                if negated {
                    &valid & &!&matches
                } else {
                    &valid & &matches
                }
            }
        })
    }
}

/// The root conjuncts of a filter, nested `and`s flattened.
fn conjuncts(raw: &Raw) -> Result<Vec<&Raw>, Error> {
    match map(raw, "a filter node")? {
        [(key, value)] if key == b"and" => Ok(list(value, "'and'")?
            .iter()
            .map(conjuncts)
            .collect::<Result<Vec<_>, _>>()?
            .into_iter()
            .flatten()
            .collect()),
        _ => Ok(vec![raw]),
    }
}

/// The matcher of `intervals` for the arrow array arrow-rs reads `column` as: the file's arrow schema can turn an INT32
/// DATE into Date64 milliseconds, a plain INT32/INT64 into a timestamp or a Date64, values unchanged.
fn bounds(
    column: &ColumnDescriptor,
    path: &[String],
    intervals: &[Interval<Scalar>],
    data_type: &DataType,
) -> Result<Matcher, Error> {
    let malformed_bounds = || malformed(format!("bounds of \"{}\" are not {data_type} values", path.join(".")));
    let signed = |factor: i64| {
        typed(intervals, |scalar| match scalar {
            Scalar::Int(value) => value.checked_mul(factor),
            _ => None,
        })
        .ok_or_else(malformed_bounds)
    };
    let unsigned = |mask: u64| {
        typed(intervals, |scalar| match scalar {
            Scalar::Int(value) => Some(*value as u64 & mask),
            _ => None,
        })
        .ok_or_else(malformed_bounds)
    };
    let floats = || {
        typed(intervals, |scalar| match scalar {
            Scalar::Float(value) => Some(*value),
            Scalar::Int(value) => Some(*value as f64),
            _ => None,
        })
        .ok_or_else(malformed_bounds)
    };
    let stored_unit = timestamp_unit(column.logical_type_ref(), column.converted_type());

    Ok(match data_type {
        DataType::Int8 => primitive::<Int8Type, _>(signed(1)?),
        DataType::Int16 => primitive::<Int16Type, _>(signed(1)?),
        DataType::Int32 => primitive::<Int32Type, _>(signed(1)?),
        DataType::Int64 => primitive::<Int64Type, _>(signed(1)?),
        DataType::Date32 => primitive::<Date32Type, _>(signed(1)?),
        DataType::Date64 if column.physical_type() == Type::INT64 => primitive::<Date64Type, _>(signed(1)?),
        DataType::Date64 => primitive::<Date64Type, _>(signed(86_400_000)?),
        DataType::Time32(TimeUnit::Second) => primitive::<Time32SecondType, _>(signed(1)?),
        DataType::Time32(_) => primitive::<Time32MillisecondType, _>(signed(1)?),
        DataType::Time64(TimeUnit::Microsecond) => primitive::<Time64MicrosecondType, _>(signed(1)?),
        DataType::Time64(_) => primitive::<Time64NanosecondType, _>(signed(1)?),
        DataType::Timestamp(unit, _) if stored_unit.is_none_or(|stored| stored == *unit) => match unit {
            TimeUnit::Second => primitive::<TimestampSecondType, _>(signed(1)?),
            TimeUnit::Millisecond => primitive::<TimestampMillisecondType, _>(signed(1)?),
            TimeUnit::Microsecond => primitive::<TimestampMicrosecondType, _>(signed(1)?),
            TimeUnit::Nanosecond => primitive::<TimestampNanosecondType, _>(signed(1)?),
        },
        DataType::UInt8 => primitive::<UInt8Type, _>(unsigned(0xFFFF_FFFF)?),
        DataType::UInt16 => primitive::<UInt16Type, _>(unsigned(0xFFFF_FFFF)?),
        DataType::UInt32 => primitive::<UInt32Type, _>(unsigned(0xFFFF_FFFF)?),
        DataType::UInt64 => primitive::<UInt64Type, _>(unsigned(u64::MAX)?),
        DataType::Float32 => primitive::<Float32Type, _>(floats()?),
        DataType::Float64 => primitive::<Float64Type, _>(floats()?),
        DataType::Boolean => {
            let intervals = typed(intervals, |scalar| match scalar {
                Scalar::Bool(value) => Some(*value),
                _ => None,
            })
            .ok_or_else(malformed_bounds)?;

            Box::new(move |array: &ArrayRef| {
                let values = array.as_boolean().values();

                ordered(values.len(), |row| values.value(row), &intervals)
            })
        }
        DataType::Decimal128(_, scale) if i32::from(*scale) == column.type_scale() => primitive::<Decimal128Type, _>(
            typed(intervals, |scalar| match scalar {
                Scalar::Int(value) => Some(i128::from(*value)),
                Scalar::Bytes(bytes) => be_i128(bytes),
                _ => None,
            })
            .ok_or_else(malformed_bounds)?,
        ),
        DataType::Decimal256(_, scale) if i32::from(*scale) == column.type_scale() => primitive::<Decimal256Type, _>(
            typed(intervals, |scalar| match scalar {
                Scalar::Int(value) => Some(i256::from_i128(i128::from(*value))),
                Scalar::Bytes(bytes) => be_i256(bytes),
                _ => None,
            })
            .ok_or_else(malformed_bounds)?,
        ),
        other => {
            let byte_array = ByteArray::of(other).ok_or_else(|| unsupported(path, other))?;
            let intervals = typed(intervals, |scalar| match scalar {
                Scalar::Bytes(bytes) => Some(bytes.clone()),
                _ => None,
            })
            .ok_or_else(malformed_bounds)?;

            by_bytes(byte_array, move |value| {
                holds(&intervals, |bound| Some(value.cmp(bound.as_slice())))
            })
        }
    })
}

/// The arrow types whose rows are bytes.
#[derive(Clone, Copy)]
enum ByteArray {
    Utf8,
    LargeUtf8,
    Utf8View,
    Binary,
    LargeBinary,
    BinaryView,
    FixedSizeBinary,
}

impl ByteArray {
    fn of(data_type: &DataType) -> Option<Self> {
        Some(match data_type {
            DataType::Utf8 => Self::Utf8,
            DataType::LargeUtf8 => Self::LargeUtf8,
            DataType::Utf8View => Self::Utf8View,
            DataType::Binary => Self::Binary,
            DataType::LargeBinary => Self::LargeBinary,
            DataType::BinaryView => Self::BinaryView,
            DataType::FixedSizeBinary(_) => Self::FixedSizeBinary,
            _ => return None,
        })
    }
}

/// A matcher testing each row's bytes.
fn by_bytes(byte_array: ByteArray, test: impl Fn(&[u8]) -> bool + Send + 'static) -> Matcher {
    macro_rules! rows {
        ($cast:ident $(::<$offset:ty>)?) => {
            Box::new(move |array: &ArrayRef| {
                let array = array.$cast$(::<$offset>)?();

                BooleanBuffer::collect_bool(array.len(), |row| {
                    let value: &[u8] = array.value(row).as_ref();

                    test(value)
                })
            })
        };
    }

    match byte_array {
        ByteArray::Utf8 => rows!(as_string::<i32>),
        ByteArray::LargeUtf8 => rows!(as_string::<i64>),
        ByteArray::Utf8View => rows!(as_string_view),
        ByteArray::Binary => rows!(as_binary::<i32>),
        ByteArray::LargeBinary => rows!(as_binary::<i64>),
        ByteArray::BinaryView => rows!(as_binary_view),
        ByteArray::FixedSizeBinary => rows!(as_fixed_size_binary),
    }
}

impl<T> Interval<T> {
    /// `order(bound)` orders the value against a bound; `None` (NaN) is in no bounded interval.
    fn contains(&self, order: impl Fn(&T) -> Option<Ordering>) -> bool {
        let lower = self.lower.as_ref().is_none_or(|bound| match order(bound) {
            Some(Ordering::Greater) => true,
            Some(Ordering::Equal) => self.lower_inclusive,
            _ => false,
        });
        let upper = self.upper.as_ref().is_none_or(|bound| match order(bound) {
            Some(Ordering::Less) => true,
            Some(Ordering::Equal) => self.upper_inclusive,
            _ => false,
        });

        lower && upper
    }
}

/// Whether the value lies in the sorted, disjoint `intervals`: only the last one starting at or below it can hold it.
fn holds<T>(intervals: &[Interval<T>], order: impl Fn(&T) -> Option<Ordering>) -> bool {
    let candidate = intervals.partition_point(|interval| {
        interval
            .lower
            .as_ref()
            .is_none_or(|bound| matches!(order(bound), Some(Ordering::Greater | Ordering::Equal)))
    });

    candidate > 0 && intervals[candidate - 1].contains(order)
}

/// The values of an `A` array, widened to the bounds' type.
fn primitive<A, T>(intervals: Vec<Interval<T>>) -> Matcher
where
    A: ArrowPrimitiveType,
    T: PartialOrd + From<A::Native> + Send + 'static,
{
    Box::new(move |array: &ArrayRef| {
        let values = array.as_primitive::<A>().values();

        ordered(values.len(), |row| T::from(values[row]), &intervals)
    })
}

/// Floats compare as IEEE 754 through `partial_cmp`: NaN orders with nothing, itself included, so it is in no interval,
/// not even an unbounded one, and -0.0 == +0.0.
fn ordered<T: PartialOrd>(rows: usize, value: impl Fn(usize) -> T, intervals: &[Interval<T>]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(rows, |row| {
        let value = value(row);

        value.partial_cmp(&value).is_some() && holds(intervals, |bound| value.partial_cmp(bound))
    })
}

/// The leaf at `path` in a predicate batch, and its rows that are not null, a struct above it included.
fn column(batch: &RecordBatch, path: &[String]) -> Result<(ArrayRef, BooleanBuffer), ArrowError> {
    let missing = || ArrowError::SchemaError(format!("The row filter batch has no leaf \"{}\"", path.join(".")));
    let (array, nulls) =
        struct_child(batch.column_by_name(&path[0]).ok_or_else(missing)?, &path[1..]).map_err(|_| missing())?;
    let valid = nulls.map_or_else(|| BooleanBuffer::new_set(array.len()), |nulls| nulls.into_inner());

    Ok((array, valid))
}

/// The parquet leaf at `path`, added to `leaves` once.
fn leaf(descr: &SchemaDescriptor, path: &[String], leaves: &mut Vec<usize>) -> Result<usize, Error> {
    let leaf = (0..descr.num_columns())
        .find(|leaf| descr.column(*leaf).path().parts() == path)
        .ok_or_else(|| not_a_leaf(path))?;

    if !leaves.contains(&leaf) {
        leaves.push(leaf);
    }

    Ok(leaf)
}

fn not_a_leaf(path: &[String]) -> Error {
    Error::Plan(format!(
        "Row filter column \"{}\" is not a leaf of the file",
        path.join(".")
    ))
}

/// The arrow type the leaf at `path` is read as.
fn data_type(schema: &Schema, path: &[String]) -> Result<DataType, Error> {
    Ok(struct_field(schema, path)
        .map_err(|_| not_a_leaf(path))?
        .0
        .data_type()
        .clone())
}

fn timestamp_unit(logical: Option<&LogicalType>, converted: ConvertedType) -> Option<TimeUnit> {
    match (logical, converted) {
        (Some(LogicalType::Timestamp(timestamp)), _) => Some(match timestamp.unit {
            parquet::basic::TimeUnit::MILLIS => TimeUnit::Millisecond,
            parquet::basic::TimeUnit::MICROS => TimeUnit::Microsecond,
            parquet::basic::TimeUnit::NANOS => TimeUnit::Nanosecond,
        }),
        (_, ConvertedType::TIMESTAMP_MILLIS) => Some(TimeUnit::Millisecond),
        (_, ConvertedType::TIMESTAMP_MICROS) => Some(TimeUnit::Microsecond),
        _ => None,
    }
}

/// Every bound converted; `None` when one has another kind.
fn typed<T>(intervals: &[Interval<Scalar>], convert: impl Fn(&Scalar) -> Option<T>) -> Option<Vec<Interval<T>>> {
    intervals
        .iter()
        .map(|interval| {
            Some(Interval {
                lower: match &interval.lower {
                    Some(bound) => Some(convert(bound)?),
                    None => None,
                },
                lower_inclusive: interval.lower_inclusive,
                upper: match &interval.upper {
                    Some(bound) => Some(convert(bound)?),
                    None => None,
                },
                upper_inclusive: interval.upper_inclusive,
            })
        })
        .collect()
}

/// Big-endian two's complement of any width, `None` when it does not fit.
fn be_i128(bytes: &[u8]) -> Option<i128> {
    sign_extended::<16>(bytes).map(i128::from_be_bytes)
}

fn be_i256(bytes: &[u8]) -> Option<i256> {
    sign_extended::<32>(bytes).map(i256::from_be_bytes)
}

fn sign_extended<const N: usize>(bytes: &[u8]) -> Option<[u8; N]> {
    let extension = if bytes.first().is_some_and(|byte| *byte >= 0x80) {
        0xFF
    } else {
        0x00
    };
    let mut bytes = bytes;

    while bytes.len() > N {
        let (first, rest) = bytes.split_first()?;

        // dropping a sign-extension byte keeps the value only when the next byte carries the same sign
        if *first != extension || (rest[0] >= 0x80) != (extension == 0xFF) {
            return None;
        }

        bytes = rest;
    }

    let mut extended = [extension; N];
    extended[N - bytes.len()..].copy_from_slice(bytes);

    Some(extended)
}

fn unsupported(path: &[String], data_type: &DataType) -> Error {
    Error::Unsupported {
        column: path.join("."),
        parquet: format!("{data_type} in a row filter"),
    }
}

fn malformed(message: String) -> Error {
    Error::Plan(format!("arrow received a malformed row filter: {message}"))
}

fn map<'a>(raw: &'a Raw, what: &str) -> Result<&'a [(Vec<u8>, Raw)], Error> {
    match raw {
        Raw::Map(entries) => Ok(entries),
        other => Err(malformed(format!(
            "{what} must be an array with string keys, got {other:?}"
        ))),
    }
}

fn list<'a>(raw: &'a Raw, what: &str) -> Result<&'a [Raw], Error> {
    match raw {
        Raw::List(entries) => Ok(entries),
        other => Err(malformed(format!("{what} must be a list, got {other:?}"))),
    }
}

fn field<'a>(entries: &'a [(Vec<u8>, Raw)], key: &[u8]) -> Result<&'a Raw, Error> {
    entries
        .iter()
        .find(|(name, _)| name == key)
        .map(|(_, value)| value)
        .ok_or_else(|| malformed(format!("missing key \"{}\"", String::from_utf8_lossy(key))))
}

fn path(raw: &Raw) -> Result<Vec<String>, Error> {
    let segments = list(raw, "a path")?
        .iter()
        .map(|segment| match segment {
            Raw::Bytes(segment) => {
                String::from_utf8(segment.clone()).map_err(|_| malformed("a path segment must be UTF-8".to_string()))
            }
            other => Err(malformed(format!("a path segment must be a string, got {other:?}"))),
        })
        .collect::<Result<Vec<_>, _>>()?;

    if segments.is_empty() {
        return Err(malformed("a path must not be empty".to_string()));
    }

    Ok(segments)
}

fn interval(raw: &Raw) -> Result<Interval<Scalar>, Error> {
    let parts = list(raw, "an interval")?;
    let [lower, lower_inclusive, upper, upper_inclusive] = parts else {
        return Err(malformed(format!("an interval has 4 parts, got {}", parts.len())));
    };
    let inclusive = |raw: &Raw| match raw {
        Raw::Bool(inclusive) => Ok(*inclusive),
        other => Err(malformed(format!(
            "an interval inclusiveness must be a bool, got {other:?}"
        ))),
    };

    Ok(Interval {
        lower: scalar(lower)?,
        lower_inclusive: inclusive(lower_inclusive)?,
        upper: scalar(upper)?,
        upper_inclusive: inclusive(upper_inclusive)?,
    })
}

fn scalar(raw: &Raw) -> Result<Option<Scalar>, Error> {
    Ok(match raw {
        Raw::Null => None,
        Raw::Bool(value) => Some(Scalar::Bool(*value)),
        Raw::Int(value) => Some(Scalar::Int(*value)),
        Raw::Float(value) if value.is_nan() => return Err(malformed("a bound must not be NaN".to_string())),
        Raw::Float(value) => Some(Scalar::Float(*value)),
        Raw::Bytes(value) => Some(Scalar::Bytes(value.clone())),
        other => return Err(malformed(format!("a bound must be a scalar, got {other:?}"))),
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{
        Array, ArrayRef, BooleanArray, Date32Array, Date64Array, Decimal128Array, FixedSizeBinaryArray, Float64Array,
        Int64Array, RecordBatch, StringArray, StructArray, TimestampSecondArray, UInt64Array,
    };
    use arrow_array::{Decimal256Array, Float32Array, Time32MillisecondArray, UInt32Array};
    use arrow_buffer::i256;
    use arrow_buffer::NullBuffer;
    use arrow_schema::{DataType, Field, Fields, TimeUnit};
    use parquet::arrow::arrow_reader::ArrowPredicate;
    use parquet::arrow::parquet_to_arrow_schema;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{Interval, PlanPredicate, Raw, Scalar};
    use crate::parquet::error::Error;

    const SCHEMA: &str = "message schema {
        optional int64 i;
        optional double f;
        optional binary s (STRING);
        optional int32 day (DATE);
        optional fixed_len_byte_array(2) d (DECIMAL(4, 2));
        optional int64 u (INTEGER(64, false));
        optional fixed_len_byte_array(16) id (UUID);
        optional group address { optional binary city (STRING); }
        optional group tags (LIST) { repeated group list { optional binary element (STRING); } }
    }";

    fn descr() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(SCHEMA).unwrap()))
    }

    fn rows(filter: &Raw, column: &str, array: ArrayRef) -> Vec<bool> {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let field = schema.field_with_name(column).unwrap().clone();

        kept(filter, &arrow_schema::Schema::new(vec![field]), &descr, array)
    }

    /// The rows every conjunct's predicate keeps.
    fn kept(filter: &Raw, schema: &arrow_schema::Schema, descr: &SchemaDescriptor, array: ArrayRef) -> Vec<bool> {
        let batch = RecordBatch::try_new(Arc::new(schema.clone()), vec![array]).unwrap();
        let mut kept = vec![true; batch.num_rows()];

        for mut predicate in PlanPredicate::conjuncts(filter, schema, descr).unwrap() {
            let truth = predicate.evaluate(batch.clone()).unwrap();

            assert_eq!(truth.null_count(), 0);

            kept = kept
                .iter()
                .zip(truth.iter())
                .map(|(kept, truth)| *kept && truth.unwrap())
                .collect();
        }

        kept
    }

    fn node(entries: Vec<(&str, Raw)>) -> Raw {
        map(entries)
    }

    fn path(path: &[&str]) -> Raw {
        Raw::List(path.iter().map(|segment| bytes(segment)).collect())
    }

    fn bound(bound: Option<Scalar>) -> Raw {
        match bound {
            None => Raw::Null,
            Some(Scalar::Int(value)) => Raw::Int(value),
            Some(Scalar::Float(value)) => Raw::Float(value),
            Some(Scalar::Bool(value)) => Raw::Bool(value),
            Some(Scalar::Bytes(value)) => Raw::Bytes(value),
        }
    }

    fn intervals_at(at: &[&str], intervals: Vec<Interval<Scalar>>) -> Raw {
        node(vec![
            ("leaf", bytes("intervals")),
            ("path", path(at)),
            (
                "intervals",
                Raw::List(
                    intervals
                        .into_iter()
                        .map(|interval| {
                            Raw::List(vec![
                                bound(interval.lower),
                                Raw::Bool(interval.lower_inclusive),
                                bound(interval.upper),
                                Raw::Bool(interval.upper_inclusive),
                            ])
                        })
                        .collect(),
                ),
            ),
        ])
    }

    fn intervals(column: &str, intervals: Vec<Interval<Scalar>>) -> Raw {
        intervals_at(&[column], intervals)
    }

    fn is_null(at: &[&str]) -> Raw {
        node(vec![("leaf", bytes("is_null")), ("path", path(at))])
    }

    fn starts_with(at: &[&str], prefix: &[u8]) -> Raw {
        node(vec![
            ("leaf", bytes("starts_with")),
            ("path", path(at)),
            ("prefix", Raw::Bytes(prefix.to_vec())),
        ])
    }

    fn absent(is_null: bool) -> Raw {
        node(vec![("leaf", bytes("absent")), ("is_null", Raw::Bool(is_null))])
    }

    fn and(nodes: Vec<Raw>) -> Raw {
        node(vec![("and", Raw::List(nodes))])
    }

    fn or(nodes: Vec<Raw>) -> Raw {
        node(vec![("or", Raw::List(nodes))])
    }

    fn below<T>(bound: T, inclusive: bool) -> Interval<T> {
        Interval {
            lower: None,
            lower_inclusive: false,
            upper: Some(bound),
            upper_inclusive: inclusive,
        }
    }

    fn point<T: Clone>(bound: T) -> Interval<T> {
        Interval {
            lower: Some(bound.clone()),
            lower_inclusive: true,
            upper: Some(bound),
            upper_inclusive: true,
        }
    }

    fn not(negated: Raw) -> Raw {
        node(vec![("not", negated)])
    }

    fn ints() -> ArrayRef {
        Arc::new(Int64Array::from(vec![None, Some(3), Some(7)]))
    }

    #[test]
    fn a_null_stays_null_under_not_and_is_never_kept() {
        let lt = intervals("i", vec![below(Scalar::Int(5), false)]);

        assert_eq!(rows(&lt, "i", ints()), [false, true, false]);
        assert_eq!(rows(&not(lt.clone()), "i", ints()), [false, false, true]);
        assert_eq!(rows(&not(not(lt)), "i", ints()), [false, true, false]);
    }

    #[test]
    fn and_and_or_follow_kleene_logic_and_swap_under_not() {
        let lt = intervals("i", vec![below(Scalar::Int(5), false)]);
        let null = is_null(&["i"]);

        assert_eq!(
            rows(&or(vec![lt.clone(), null.clone()]), "i", ints()),
            [true, true, false]
        );
        assert_eq!(
            rows(&not(and(vec![lt.clone(), not(null.clone())])), "i", ints()),
            [true, false, true]
        );
        assert_eq!(rows(&not(or(vec![lt, null])), "i", ints()), [false, false, true]);
        assert_eq!(rows(&and(vec![]), "i", ints()), [true, true, true]);
        assert_eq!(rows(&not(and(vec![])), "i", ints()), [false, false, false]);
        assert_eq!(rows(&or(vec![]), "i", ints()), [false, false, false]);
    }

    #[test]
    fn nan_is_in_no_interval_so_every_negation_keeps_it() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(3.0), Some(5.0), None]));
        let lt = intervals("f", vec![below(Scalar::Float(5.0), false)]);
        let unbounded = intervals(
            "f",
            vec![Interval {
                lower: None,
                lower_inclusive: false,
                upper: None,
                upper_inclusive: false,
            }],
        );

        assert_eq!(rows(&lt, "f", Arc::clone(&floats)), [false, true, false, false]);
        assert_eq!(rows(&not(lt), "f", Arc::clone(&floats)), [true, false, true, false]);
        assert_eq!(
            rows(
                &not(intervals("f", vec![point(Scalar::Float(5.0))])),
                "f",
                Arc::clone(&floats)
            ),
            [true, true, false, false]
        );
        assert_eq!(rows(&unbounded, "f", floats), [false, true, true, false]);
    }

    #[test]
    fn negative_zero_equals_positive_zero() {
        let zeros: ArrayRef = Arc::new(Float64Array::from(vec![-0.0, 0.0]));

        assert_eq!(
            rows(
                &intervals("f", vec![point(Scalar::Float(0.0))]),
                "f",
                Arc::clone(&zeros)
            ),
            [true, true]
        );
        assert_eq!(
            rows(&intervals("f", vec![below(Scalar::Float(0.0), false)]), "f", zeros),
            [false, false]
        );
    }

    #[test]
    fn a_decimal_bound_of_any_width_compares_with_the_unscaled_value() {
        let decimals: ArrayRef = Arc::new(
            Decimal128Array::from(vec![Some(1234), Some(1233), Some(-1234), None])
                .with_precision_and_scale(4, 2)
                .unwrap(),
        );
        let wide = Scalar::Bytes(vec![0, 0, 0, 0, 0, 0, 0x04, 0xD2]);

        assert_eq!(
            rows(&intervals("d", vec![point(wide.clone())]), "d", Arc::clone(&decimals)),
            [true, false, false, false]
        );
        assert_eq!(
            rows(&intervals("d", vec![below(wide, false)]), "d", Arc::clone(&decimals)),
            [false, true, true, false]
        );
        assert_eq!(
            rows(
                &intervals("d", vec![below(Scalar::Bytes(vec![0xFF, 0x00]), true)]),
                "d",
                decimals
            ),
            [false, false, true, false]
        );
    }

    #[test]
    fn a_date_bound_is_its_day_number() {
        let days: ArrayRef = Arc::new(Date32Array::from(vec![1, 2, 3]));

        assert_eq!(
            rows(&intervals("day", vec![point(Scalar::Int(2))]), "day", days),
            [false, true, false]
        );
    }

    #[test]
    fn an_unsigned_64_bit_pattern_compares_unsigned() {
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![5, u64::MAX]));

        assert_eq!(
            rows(&intervals("u", vec![below(Scalar::Int(-1), false)]), "u", big),
            [true, false]
        );
    }

    #[test]
    fn bytes_compare_unsigned_and_prefixes_match_raw_bytes() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("ź"), Some(""), None]));
        let prefix = |prefix: &str| starts_with(&["s"], prefix.as_bytes());

        assert_eq!(
            rows(
                &intervals("s", vec![below(Scalar::Bytes(b"z".to_vec()), false)]),
                "s",
                Arc::clone(&strings)
            ),
            [true, false, true, false]
        );
        assert_eq!(rows(&prefix(""), "s", Arc::clone(&strings)), [true, true, true, false]);
        assert_eq!(
            rows(&not(prefix("")), "s", Arc::clone(&strings)),
            [false, false, false, false]
        );
        assert_eq!(
            rows(&prefix("\u{c5}"), "s", Arc::clone(&strings)),
            [false, false, false, false]
        );
        assert_eq!(
            rows(&starts_with(&["s"], &[0xC5]), "s", strings),
            [false, true, false, false]
        );
    }

    #[test]
    fn a_uuid_compares_its_sixteen_bytes() {
        let ids: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![[1u8; 16], [2u8; 16]].into_iter()).unwrap());

        assert_eq!(
            rows(&intervals("id", vec![point(Scalar::Bytes(vec![2; 16]))]), "id", ids),
            [false, true]
        );
    }

    #[test]
    fn an_absent_column_is_null_in_every_row() {
        assert_eq!(rows(&absent(false), "i", ints()), [false, false, false]);
        assert_eq!(rows(&not(absent(false)), "i", ints()), [false, false, false]);
        assert_eq!(rows(&absent(true), "i", ints()), [true, true, true]);
        assert_eq!(rows(&not(absent(true)), "i", ints()), [false, false, false]);
    }

    #[test]
    fn a_value_is_found_among_sorted_intervals() {
        let values: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 5, 6, 8, 9]));
        let above = Interval {
            lower: Some(Scalar::Int(8)),
            lower_inclusive: false,
            upper: None,
            upper_inclusive: false,
        };

        assert_eq!(
            rows(
                &intervals("i", vec![below(Scalar::Int(2), false), point(Scalar::Int(5)), above]),
                "i",
                values
            ),
            [true, false, true, false, false, true]
        );
    }

    #[test]
    fn each_root_conjunct_is_its_own_predicate_reading_its_own_leaves() {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let null = |column: &str| is_null(&[column]);
        let predicates = PlanPredicate::conjuncts(
            &and(vec![null("s"), and(vec![null("i"), or(vec![null("f"), null("i")])])]),
            &schema,
            &descr,
        )
        .unwrap();

        assert_eq!(
            predicates.iter().map(PlanPredicate::leaves).collect::<Vec<_>>(),
            [&[2][..], &[0], &[1, 0]]
        );
        assert!(PlanPredicate::conjuncts(&and(vec![]), &schema, &descr)
            .unwrap()
            .is_empty());
    }

    #[test]
    fn every_primitive_kind_compares_in_its_own_type() {
        let descr = SchemaDescriptor::new(Arc::new(
            parse_message_type(
                "message schema {
                    optional int32 u32 (INTEGER(32, false));
                    optional float f32;
                    optional int32 t (TIME(MILLIS, true));
                    optional fixed_len_byte_array(17) d (DECIMAL(40, 2));
                }",
            )
            .unwrap(),
        ));
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let single = |name: &str| arrow_schema::Schema::new(vec![schema.field_with_name(name).unwrap().clone()]);
        let cases: Vec<(Raw, &str, ArrayRef, Vec<bool>)> = vec![
            (
                intervals("u32", vec![below(Scalar::Int(-1), false)]),
                "u32",
                Arc::new(UInt32Array::from(vec![5, u32::MAX])),
                vec![true, false],
            ),
            (
                not(intervals("f32", vec![below(Scalar::Float(5.0), false)])),
                "f32",
                Arc::new(Float32Array::from(vec![f32::NAN, 3.0, 5.0])),
                vec![true, false, true],
            ),
            (
                intervals("t", vec![point(Scalar::Int(2_000))]),
                "t",
                Arc::new(Time32MillisecondArray::from(vec![1_000, 2_000])),
                vec![false, true],
            ),
            (
                intervals("d", vec![below(Scalar::Bytes(vec![0x04, 0xD2]), false)]),
                "d",
                Arc::new(
                    Decimal256Array::from(vec![i256::from_i128(1234), i256::from_i128(-1234)])
                        .with_precision_and_scale(40, 2)
                        .unwrap(),
                ),
                vec![false, true],
            ),
        ];

        for (filter, column, array, expected) in cases {
            assert_eq!(kept(&filter, &single(column), &descr, array), expected, "{column}");
        }
    }

    #[test]
    fn a_date64_hint_turns_day_bounds_into_milliseconds_and_a_timestamp_hint_keeps_plain_integers() {
        let descr = SchemaDescriptor::new(Arc::new(
            parse_message_type("message schema { optional int32 day (DATE); optional int64 seconds; }").unwrap(),
        ));
        let day = Field::new("day", DataType::Date64, true);
        let seconds = Field::new("seconds", DataType::Timestamp(TimeUnit::Second, None), true);

        assert_eq!(
            kept(
                &intervals("day", vec![point(Scalar::Int(2))]),
                &arrow_schema::Schema::new(vec![day]),
                &descr,
                Arc::new(Date64Array::from(vec![86_400_000, 172_800_000, 172_800_001]))
            ),
            [false, true, false]
        );
        assert_eq!(
            kept(
                &intervals("seconds", vec![below(Scalar::Int(2), true)]),
                &arrow_schema::Schema::new(vec![seconds]),
                &descr,
                Arc::new(TimestampSecondArray::from(vec![2, 3]))
            ),
            [true, false]
        );
    }

    #[test]
    fn a_struct_leaf_is_null_where_its_struct_is() {
        let cities = Arc::new(StringArray::from(vec![Some("a"), Some("a")])) as ArrayRef;
        let address: ArrayRef = Arc::new(StructArray::new(
            Fields::from(vec![Field::new("city", DataType::Utf8, true)]),
            vec![cities],
            Some(NullBuffer::from(vec![true, false])),
        ));
        let city = intervals_at(&["address", "city"], vec![point(Scalar::Bytes(b"a".to_vec()))]);
        let null = is_null(&["address", "city"]);

        assert_eq!(rows(&city, "address", Arc::clone(&address)), [true, false]);
        assert_eq!(rows(&not(city), "address", Arc::clone(&address)), [false, false]);
        assert_eq!(rows(&null, "address", address), [false, true]);
    }

    #[test]
    fn booleans_order_false_before_true() {
        let flags: ArrayRef = Arc::new(BooleanArray::from(vec![false, true]));
        let descr = SchemaDescriptor::new(Arc::new(
            parse_message_type("message schema { optional boolean b; }").unwrap(),
        ));
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();

        assert_eq!(
            kept(
                &intervals("b", vec![below(Scalar::Bool(true), false)]),
                &schema,
                &descr,
                flags
            ),
            [true, false]
        );
    }

    #[test]
    fn a_bound_of_another_kind_than_the_column_or_a_path_that_is_no_leaf_is_refused() {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let refused = |filter: Raw| match PlanPredicate::conjuncts(&filter, &schema, &descr) {
            Err(Error::Plan(message)) => message,
            _ => panic!("the filter was not refused"),
        };

        assert_eq!(
            refused(intervals("i", vec![point(Scalar::Bytes(b"1".to_vec()))])),
            "arrow received a malformed row filter: bounds of \"i\" are not Int64 values"
        );
        assert_eq!(
            refused(is_null(&["missing"])),
            "Row filter column \"missing\" is not a leaf of the file"
        );
        assert_eq!(
            refused(is_null(&["i", "deeper"])),
            "Row filter column \"i.deeper\" is not a leaf of the file"
        );
        assert_eq!(
            refused(is_null(&["tags", "list", "element"])),
            "Row filter column \"tags.list.element\" is not a leaf of the file"
        );
    }

    fn map(entries: Vec<(&str, Raw)>) -> Raw {
        Raw::Map(
            entries
                .into_iter()
                .map(|(key, value)| (key.as_bytes().to_vec(), value))
                .collect(),
        )
    }

    fn bytes(value: &str) -> Raw {
        Raw::Bytes(value.as_bytes().to_vec())
    }

    #[test]
    fn the_converter_shape_compiles_into_one_predicate_per_root_conjunct() {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let predicates = PlanPredicate::conjuncts(
            &and(vec![
                not(absent(true)),
                or(vec![intervals_at(
                    &["address", "city"],
                    vec![Interval {
                        lower: Some(Scalar::Bytes(b"a".to_vec())),
                        lower_inclusive: true,
                        upper: None,
                        upper_inclusive: false,
                    }],
                )]),
                is_null(&["i"]),
                starts_with(&["s"], b"p"),
                and(vec![]),
            ]),
            &schema,
            &descr,
        )
        .unwrap();

        assert_eq!(
            predicates.iter().map(PlanPredicate::leaves).collect::<Vec<_>>(),
            [&[][..], &[7], &[0], &[2]]
        );
    }

    #[test]
    fn an_unknown_leaf_or_a_missing_key_is_refused() {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let refused = |filter: Raw| match PlanPredicate::conjuncts(&filter, &schema, &descr) {
            Err(Error::Plan(message)) => message.replace("arrow received a malformed row filter: ", ""),
            _ => panic!("the filter was not refused"),
        };

        assert_eq!(refused(map(vec![("xor", Raw::List(vec![]))])), "missing key \"leaf\"");
        assert!(refused(map(vec![("leaf", bytes("between"))])).starts_with("unknown leaf"));
        assert_eq!(refused(map(vec![("leaf", bytes("is_null"))])), "missing key \"path\"");
        assert_eq!(refused(map(vec![("leaf", bytes("absent"))])), "missing key \"is_null\"");
        assert_eq!(
            refused(map(vec![
                ("leaf", bytes("intervals")),
                ("path", Raw::List(vec![bytes("f")])),
                (
                    "intervals",
                    Raw::List(vec![Raw::List(vec![
                        Raw::Float(f64::NAN),
                        Raw::Bool(true),
                        Raw::Null,
                        Raw::Bool(false)
                    ])])
                ),
            ])),
            "a bound must not be NaN"
        );
    }
}
