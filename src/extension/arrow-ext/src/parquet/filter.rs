//! The row filter `ReadPlanConverter::toExtension()` sends as `'filter'`, evaluated by arrow-rs's push decoder on the
//! predicate leaves of each row group before the projected leaves are decoded, one predicate per root conjunct. A row
//! is kept only when the filter is TRUE: `Not` is pushed down to the leaves as a polarity, so every node yields the
//! rows it is TRUE for, and a NULL (a null value, an absent column) is TRUE in neither polarity. Floats compare as
//! IEEE 754: NaN is false, -0.0 equals +0.0.

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
use crate::parquet::read::child;

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
pub enum Scalar {
    Int(i64),
    Float(f64),
    Bool(bool),
    Bytes(Vec<u8>),
}

/// A null bound is unbounded.
#[derive(Debug, Clone, PartialEq)]
pub struct Interval<T> {
    pub lower: Option<T>,
    pub lower_inclusive: bool,
    pub upper: Option<T>,
    pub upper_inclusive: bool,
}

#[derive(Debug, Clone, PartialEq)]
pub enum FilterNode {
    And(Vec<FilterNode>),
    Or(Vec<FilterNode>),
    Not(Box<FilterNode>),
    /// Sorted, disjoint intervals.
    Intervals {
        path: Vec<String>,
        physical: Type,
        intervals: Vec<Interval<Scalar>>,
    },
    IsNull {
        path: Vec<String>,
    },
    StartsWith {
        path: Vec<String>,
        prefix: Vec<u8>,
    },
    /// A leaf on a column absent from the file, null in every row: TRUE for an IS NULL, NULL for any other leaf.
    Absent {
        is_null: bool,
    },
}

impl FilterNode {
    pub fn parse(raw: &Raw) -> Result<Self, Error> {
        let entries = map(raw, "a filter node")?;

        if let [(key, value)] = entries {
            match key.as_slice() {
                b"and" => {
                    return Ok(Self::And(
                        list(value, "'and'")?
                            .iter()
                            .map(Self::parse)
                            .collect::<Result<_, _>>()?,
                    ))
                }
                b"or" => {
                    return Ok(Self::Or(
                        list(value, "'or'")?.iter().map(Self::parse).collect::<Result<_, _>>()?,
                    ))
                }
                b"not" => return Ok(Self::Not(Box::new(Self::parse(value)?))),
                _ => {}
            }
        }

        Ok(match field(entries, b"leaf")? {
            Raw::Bytes(kind) if kind == b"intervals" => Self::Intervals {
                path: path(field(entries, b"path")?)?,
                physical: physical(field(entries, b"physical")?)?,
                intervals: list(field(entries, b"intervals")?, "'intervals'")?
                    .iter()
                    .map(interval)
                    .collect::<Result<_, _>>()?,
            },
            Raw::Bytes(kind) if kind == b"is_null" => Self::IsNull {
                path: path(field(entries, b"path")?)?,
            },
            Raw::Bytes(kind) if kind == b"starts_with" => Self::StartsWith {
                path: path(field(entries, b"path")?)?,
                prefix: match field(entries, b"prefix")? {
                    Raw::Bytes(prefix) => prefix.clone(),
                    other => return Err(malformed(format!("a prefix must be a string, got {other:?}"))),
                },
            },
            Raw::Bytes(kind) if kind == b"absent" => Self::Absent {
                is_null: match field(entries, b"is_null")? {
                    Raw::Bool(is_null) => *is_null,
                    other => return Err(malformed(format!("'is_null' must be a bool, got {other:?}"))),
                },
            },
            leaf => return Err(malformed(format!("unknown leaf {leaf:?}"))),
        })
    }

    /// The root conjuncts, nested `And`s flattened.
    fn conjuncts(&self) -> Vec<&Self> {
        match self {
            Self::And(children) => children.iter().flat_map(Self::conjuncts).collect(),
            other => vec![other],
        }
    }
}

/// `$collect(rows, value, $argument)` with `value` the bytes of a row of `$array`, by the array's byte type; `None` for
/// any other type.
macro_rules! by_bytes {
    ($array:expr, $collect:ident, $argument:expr) => {{
        let array = $array;
        let rows = array.len();

        match array.data_type() {
            DataType::Utf8 => {
                let array = array.as_string::<i32>();
                Some($collect(rows, |row| array.value(row).as_bytes(), $argument))
            }
            DataType::LargeUtf8 => {
                let array = array.as_string::<i64>();
                Some($collect(rows, |row| array.value(row).as_bytes(), $argument))
            }
            DataType::Utf8View => {
                let array = array.as_string_view();
                Some($collect(rows, |row| array.value(row).as_bytes(), $argument))
            }
            DataType::Binary => {
                let array = array.as_binary::<i32>();
                Some($collect(rows, |row| array.value(row), $argument))
            }
            DataType::LargeBinary => {
                let array = array.as_binary::<i64>();
                Some($collect(rows, |row| array.value(row), $argument))
            }
            DataType::BinaryView => {
                let array = array.as_binary_view();
                Some($collect(rows, |row| array.value(row), $argument))
            }
            DataType::FixedSizeBinary(_) => {
                let array = array.as_fixed_size_binary();
                Some($collect(rows, |row| array.value(row), $argument))
            }
            _ => None,
        }
    }};
}

/// The `ArrowPredicate` of one root conjunct, checked against the file's arrow schema when built.
pub struct PlanPredicate {
    leaves: Vec<usize>,
    mask: ProjectionMask,
    root: Compiled,
}

impl PlanPredicate {
    /// One predicate per root conjunct of `filter`, in order: arrow-rs evaluates each on the rows the ones before kept.
    pub fn conjuncts(filter: &FilterNode, schema: &Schema, descr: &SchemaDescriptor) -> Result<Vec<Self>, Error> {
        filter
            .conjuncts()
            .into_iter()
            .map(|conjunct| {
                let mut leaves = Vec::new();
                let root = Compiled::new(conjunct, schema, descr, &mut leaves)?;

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

/// A filter node with each leaf's bounds typed for the arrow array the leaf is read as.
enum Compiled {
    And(Vec<Compiled>),
    Or(Vec<Compiled>),
    Not(Box<Compiled>),
    Intervals { path: Vec<String>, bounds: Bounds },
    IsNull { path: Vec<String> },
    StartsWith { path: Vec<String>, prefix: Vec<u8> },
    Absent { is_null: bool },
}

enum Bounds {
    Signed(Vec<Interval<i64>>),
    Unsigned(Vec<Interval<u64>>),
    Float(Vec<Interval<f64>>),
    Boolean(Vec<Interval<bool>>),
    Decimal128(Vec<Interval<i128>>),
    Decimal256(Vec<Interval<i256>>),
    Bytes(Vec<Interval<Vec<u8>>>),
}

impl Compiled {
    /// `leaves` gains the parquet leaf of each path the node reads, once.
    fn new(
        node: &FilterNode,
        schema: &Schema,
        descr: &SchemaDescriptor,
        leaves: &mut Vec<usize>,
    ) -> Result<Self, Error> {
        let mut children = |nodes: &[FilterNode]| {
            nodes
                .iter()
                .map(|node| Self::new(node, schema, descr, leaves))
                .collect::<Result<Vec<_>, _>>()
        };

        Ok(match node {
            FilterNode::And(nodes) => Self::And(children(nodes)?),
            FilterNode::Or(nodes) => Self::Or(children(nodes)?),
            FilterNode::Not(node) => Self::Not(Box::new(Self::new(node, schema, descr, leaves)?)),
            FilterNode::Intervals {
                path,
                physical,
                intervals,
            } => Self::Intervals {
                path: path.clone(),
                bounds: Bounds::new(
                    &descr.column(leaf(descr, path, leaves)?),
                    path,
                    *physical,
                    intervals,
                    schema,
                )?,
            },
            FilterNode::IsNull { path } => {
                leaf(descr, path, leaves)?;

                Self::IsNull { path: path.clone() }
            }
            FilterNode::StartsWith { path, prefix } => {
                leaf(descr, path, leaves)?;
                let data_type = data_type(schema, path)?;

                if !is_bytes(&data_type) {
                    return Err(unsupported(path, &data_type));
                }

                Self::StartsWith {
                    path: path.clone(),
                    prefix: prefix.clone(),
                }
            }
            FilterNode::Absent { is_null } => Self::Absent { is_null: *is_null },
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
                let valid = valid(&column(batch, path)?);

                if negated {
                    valid
                } else {
                    !&valid
                }
            }
            Self::StartsWith { path, prefix } => {
                let array = column(batch, path)?;
                let matches = by_bytes!(&array, bytes, prefix.as_slice())
                    .ok_or_else(|| ArrowError::InvalidArgumentError(format!("{} has no prefix", array.data_type())))?;

                polar(&valid(&array), &matches, negated)
            }
            Self::Intervals { path, bounds } => {
                let array = column(batch, path)?;

                polar(&valid(&array), &bounds.matches(&array)?, negated)
            }
        })
    }
}

impl Bounds {
    /// Bounds in the unit of the arrow array arrow-rs reads `column` as: the file's arrow schema can turn an INT32
    /// DATE into Date64 milliseconds, a plain INT32/INT64 into a timestamp or a Date64, values unchanged.
    fn new(
        column: &ColumnDescriptor,
        path: &[String],
        physical: Type,
        intervals: &[Interval<Scalar>],
        schema: &Schema,
    ) -> Result<Self, Error> {
        let data_type = data_type(schema, path)?;
        let malformed_bounds = || malformed(format!("bounds of \"{}\" are not {data_type} values", path.join(".")));

        if column.physical_type() != physical {
            return Err(Error::Plan(format!(
                "Row filter column \"{}\" is {}, not {physical}",
                path.join("."),
                column.physical_type()
            )));
        }

        let ints = |factor: i64| {
            typed(intervals, |scalar| match scalar {
                Scalar::Int(value) => value.checked_mul(factor),
                _ => None,
            })
            .ok_or_else(malformed_bounds)
        };

        Ok(match (&data_type, physical) {
            (
                DataType::Int8
                | DataType::Int16
                | DataType::Int32
                | DataType::Int64
                | DataType::Date32
                | DataType::Time32(_)
                | DataType::Time64(_),
                _,
            )
            | (DataType::Date64, Type::INT64) => Self::Signed(ints(1)?),
            (DataType::Date64, _) => Self::Signed(ints(86_400_000)?),
            (DataType::Timestamp(unit, _), _)
                if timestamp_unit(column.logical_type_ref(), column.converted_type())
                    .is_none_or(|stored| stored == *unit) =>
            {
                Self::Signed(ints(1)?)
            }
            (DataType::UInt8 | DataType::UInt16 | DataType::UInt32, _) => Self::Unsigned(
                typed(intervals, |scalar| match scalar {
                    Scalar::Int(value) => Some(*value as u64 & 0xFFFF_FFFF),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (DataType::UInt64, _) => Self::Unsigned(
                typed(intervals, |scalar| match scalar {
                    Scalar::Int(value) => Some(*value as u64),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (DataType::Float32 | DataType::Float64, _) => Self::Float(
                typed(intervals, |scalar| match scalar {
                    Scalar::Float(value) => Some(*value),
                    Scalar::Int(value) => Some(*value as f64),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (DataType::Boolean, _) => Self::Boolean(
                typed(intervals, |scalar| match scalar {
                    Scalar::Bool(value) => Some(*value),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (DataType::Decimal128(_, scale), _) if i32::from(*scale) == column.type_scale() => Self::Decimal128(
                typed(intervals, |scalar| match scalar {
                    Scalar::Int(value) => Some(i128::from(*value)),
                    Scalar::Bytes(bytes) => be_i128(bytes),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (DataType::Decimal256(_, scale), _) if i32::from(*scale) == column.type_scale() => Self::Decimal256(
                typed(intervals, |scalar| match scalar {
                    Scalar::Int(value) => Some(i256::from_i128(i128::from(*value))),
                    Scalar::Bytes(bytes) => be_i256(bytes),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            (data_type, _) if is_bytes(data_type) => Self::Bytes(
                typed(intervals, |scalar| match scalar {
                    Scalar::Bytes(bytes) => Some(bytes.clone()),
                    _ => None,
                })
                .ok_or_else(malformed_bounds)?,
            ),
            _ => return Err(unsupported(path, &data_type)),
        })
    }

    /// Per row, whether its value lies in an interval; a null row's slot holds no value and is masked by its caller.
    fn matches(&self, array: &ArrayRef) -> Result<BooleanBuffer, ArrowError> {
        Ok(match (self, array.data_type()) {
            (Self::Signed(intervals), DataType::Int8) => primitive::<Int8Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Int16) => primitive::<Int16Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Int32) => primitive::<Int32Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Int64) => primitive::<Int64Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Date32) => primitive::<Date32Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Date64) => primitive::<Date64Type, _>(array, intervals),
            (Self::Signed(intervals), DataType::Time32(TimeUnit::Second)) => {
                primitive::<Time32SecondType, _>(array, intervals)
            }
            (Self::Signed(intervals), DataType::Time32(_)) => primitive::<Time32MillisecondType, _>(array, intervals),
            (Self::Signed(intervals), DataType::Time64(TimeUnit::Microsecond)) => {
                primitive::<Time64MicrosecondType, _>(array, intervals)
            }
            (Self::Signed(intervals), DataType::Time64(_)) => primitive::<Time64NanosecondType, _>(array, intervals),
            (Self::Signed(intervals), DataType::Timestamp(TimeUnit::Second, _)) => {
                primitive::<TimestampSecondType, _>(array, intervals)
            }
            (Self::Signed(intervals), DataType::Timestamp(TimeUnit::Millisecond, _)) => {
                primitive::<TimestampMillisecondType, _>(array, intervals)
            }
            (Self::Signed(intervals), DataType::Timestamp(TimeUnit::Microsecond, _)) => {
                primitive::<TimestampMicrosecondType, _>(array, intervals)
            }
            (Self::Signed(intervals), DataType::Timestamp(TimeUnit::Nanosecond, _)) => {
                primitive::<TimestampNanosecondType, _>(array, intervals)
            }
            (Self::Unsigned(intervals), DataType::UInt8) => primitive::<UInt8Type, _>(array, intervals),
            (Self::Unsigned(intervals), DataType::UInt16) => primitive::<UInt16Type, _>(array, intervals),
            (Self::Unsigned(intervals), DataType::UInt32) => primitive::<UInt32Type, _>(array, intervals),
            (Self::Unsigned(intervals), DataType::UInt64) => primitive::<UInt64Type, _>(array, intervals),
            (Self::Float(intervals), DataType::Float32) => primitive::<Float32Type, _>(array, intervals),
            (Self::Float(intervals), DataType::Float64) => primitive::<Float64Type, _>(array, intervals),
            (Self::Decimal128(intervals), DataType::Decimal128(_, _)) => {
                primitive::<Decimal128Type, _>(array, intervals)
            }
            (Self::Decimal256(intervals), DataType::Decimal256(_, _)) => {
                primitive::<Decimal256Type, _>(array, intervals)
            }
            (Self::Boolean(intervals), DataType::Boolean) => {
                let values = array.as_boolean().values();

                ordered(values.len(), |row| values.value(row), intervals)
            }
            (Self::Bytes(intervals), _) => {
                by_bytes!(array, sliced, intervals).ok_or_else(|| mismatch(array.data_type()))?
            }
            (_, other) => return Err(mismatch(other)),
        })
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

/// Rows TRUE under the polarity: a valid row whose match is `!negated`.
fn polar(valid: &BooleanBuffer, matches: &BooleanBuffer, negated: bool) -> BooleanBuffer {
    if negated {
        valid & &!matches
    } else {
        valid & matches
    }
}

fn valid(array: &ArrayRef) -> BooleanBuffer {
    array
        .logical_nulls()
        .map_or_else(|| BooleanBuffer::new_set(array.len()), |nulls| nulls.inner().clone())
}

/// `array`'s values, cast to `A` once, widened to the bounds' type.
fn primitive<A: ArrowPrimitiveType, T: PartialOrd + From<A::Native>>(
    array: &ArrayRef,
    intervals: &[Interval<T>],
) -> BooleanBuffer {
    let values = array.as_primitive::<A>().values();

    ordered(values.len(), |row| T::from(values[row]), intervals)
}

/// Floats compare as IEEE 754 through `partial_cmp`: NaN orders with nothing, itself included, so it is in no interval,
/// not even an unbounded one, and -0.0 == +0.0.
fn ordered<T: PartialOrd>(rows: usize, value: impl Fn(usize) -> T, intervals: &[Interval<T>]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(rows, |row| {
        let value = value(row);

        value.partial_cmp(&value).is_some() && holds(intervals, |bound| value.partial_cmp(bound))
    })
}

/// Bytes compare unsigned, lexicographically.
fn sliced<'a>(rows: usize, value: impl Fn(usize) -> &'a [u8], intervals: &[Interval<Vec<u8>>]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(rows, |row| {
        let value = value(row);

        holds(intervals, |bound| Some(value.cmp(bound.as_slice())))
    })
}

fn bytes<'a>(rows: usize, value: impl Fn(usize) -> &'a [u8], prefix: &[u8]) -> BooleanBuffer {
    BooleanBuffer::collect_bool(rows, |row| value(row).starts_with(prefix))
}

fn is_bytes(data_type: &DataType) -> bool {
    matches!(
        data_type,
        DataType::Utf8
            | DataType::LargeUtf8
            | DataType::Utf8View
            | DataType::Binary
            | DataType::LargeBinary
            | DataType::BinaryView
            | DataType::FixedSizeBinary(_)
    )
}

/// The leaf at `path` in a predicate batch, null wherever a struct above it is.
fn column(batch: &RecordBatch, path: &[String]) -> Result<ArrayRef, ArrowError> {
    let missing = || ArrowError::SchemaError(format!("The row filter batch has no leaf \"{}\"", path.join(".")));
    let root = batch.column_by_name(&path[0]).ok_or_else(missing)?;

    child(root, &path[1..]).map_err(|error| match error {
        Error::Arrow(error) => error,
        _ => missing(),
    })
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
    let missing = || not_a_leaf(path);
    let mut data_type = schema.field_with_name(&path[0]).map_err(|_| missing())?.data_type();

    for name in &path[1..] {
        data_type = match data_type {
            DataType::Struct(children) => children.find(name).ok_or_else(missing)?.1.data_type(),
            _ => return Err(missing()),
        };
    }

    Ok(data_type.clone())
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

fn mismatch(data_type: &DataType) -> ArrowError {
    ArrowError::InvalidArgumentError(format!("The row filter has no bounds for {data_type}"))
}

fn malformed(message: String) -> Error {
    Error::Filter(message)
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

fn physical(raw: &Raw) -> Result<Type, Error> {
    Ok(match raw {
        Raw::Bytes(name) => match name.as_slice() {
            b"BOOLEAN" => Type::BOOLEAN,
            b"INT32" => Type::INT32,
            b"INT64" => Type::INT64,
            b"FLOAT" => Type::FLOAT,
            b"DOUBLE" => Type::DOUBLE,
            b"BYTE_ARRAY" => Type::BYTE_ARRAY,
            b"FIXED_LEN_BYTE_ARRAY" => Type::FIXED_LEN_BYTE_ARRAY,
            _ => {
                return Err(malformed(format!(
                    "unknown physical type \"{}\"",
                    String::from_utf8_lossy(name)
                )))
            }
        },
        other => return Err(malformed(format!("a physical type must be a string, got {other:?}"))),
    })
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
    use parquet::basic::Type;
    use parquet::schema::parser::parse_message_type;
    use parquet::schema::types::SchemaDescriptor;

    use super::{FilterNode, Interval, PlanPredicate, Raw, Scalar};
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
    }";

    fn descr() -> SchemaDescriptor {
        SchemaDescriptor::new(Arc::new(parse_message_type(SCHEMA).unwrap()))
    }

    fn rows(filter: &FilterNode, column: &str, array: ArrayRef) -> Vec<bool> {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();
        let field = schema.field_with_name(column).unwrap().clone();

        kept(filter, &arrow_schema::Schema::new(vec![field]), &descr, array)
    }

    /// The rows every conjunct's predicate keeps.
    fn kept(
        filter: &FilterNode,
        schema: &arrow_schema::Schema,
        descr: &SchemaDescriptor,
        array: ArrayRef,
    ) -> Vec<bool> {
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

    fn intervals(column: &str, physical: Type, intervals: Vec<Interval<Scalar>>) -> FilterNode {
        FilterNode::Intervals {
            path: vec![column.to_string()],
            physical,
            intervals,
        }
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

    fn not(node: FilterNode) -> FilterNode {
        FilterNode::Not(Box::new(node))
    }

    fn ints() -> ArrayRef {
        Arc::new(Int64Array::from(vec![None, Some(3), Some(7)]))
    }

    #[test]
    fn a_null_stays_null_under_not_and_is_never_kept() {
        let lt = intervals("i", Type::INT64, vec![below(Scalar::Int(5), false)]);

        assert_eq!(rows(&lt, "i", ints()), [false, true, false]);
        assert_eq!(rows(&not(lt.clone()), "i", ints()), [false, false, true]);
        assert_eq!(rows(&not(not(lt)), "i", ints()), [false, true, false]);
    }

    #[test]
    fn and_and_or_follow_kleene_logic_and_swap_under_not() {
        let lt = intervals("i", Type::INT64, vec![below(Scalar::Int(5), false)]);
        let null = FilterNode::IsNull {
            path: vec!["i".to_string()],
        };

        assert_eq!(
            rows(&FilterNode::Or(vec![lt.clone(), null.clone()]), "i", ints()),
            [true, true, false]
        );
        assert_eq!(
            rows(&not(FilterNode::And(vec![lt.clone(), not(null.clone())])), "i", ints()),
            [true, false, true]
        );
        assert_eq!(
            rows(&not(FilterNode::Or(vec![lt, null])), "i", ints()),
            [false, false, true]
        );
        assert_eq!(rows(&FilterNode::And(vec![]), "i", ints()), [true, true, true]);
        assert_eq!(rows(&not(FilterNode::And(vec![])), "i", ints()), [false, false, false]);
        assert_eq!(rows(&FilterNode::Or(vec![]), "i", ints()), [false, false, false]);
    }

    #[test]
    fn nan_is_in_no_interval_so_every_negation_keeps_it() {
        let floats: ArrayRef = Arc::new(Float64Array::from(vec![Some(f64::NAN), Some(3.0), Some(5.0), None]));
        let lt = intervals("f", Type::DOUBLE, vec![below(Scalar::Float(5.0), false)]);
        let unbounded = intervals(
            "f",
            Type::DOUBLE,
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
                &not(intervals("f", Type::DOUBLE, vec![point(Scalar::Float(5.0))])),
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
                &intervals("f", Type::DOUBLE, vec![point(Scalar::Float(0.0))]),
                "f",
                Arc::clone(&zeros)
            ),
            [true, true]
        );
        assert_eq!(
            rows(
                &intervals("f", Type::DOUBLE, vec![below(Scalar::Float(0.0), false)]),
                "f",
                zeros
            ),
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
            rows(
                &intervals("d", Type::FIXED_LEN_BYTE_ARRAY, vec![point(wide.clone())]),
                "d",
                Arc::clone(&decimals)
            ),
            [true, false, false, false]
        );
        assert_eq!(
            rows(
                &intervals("d", Type::FIXED_LEN_BYTE_ARRAY, vec![below(wide, false)]),
                "d",
                Arc::clone(&decimals)
            ),
            [false, true, true, false]
        );
        assert_eq!(
            rows(
                &intervals(
                    "d",
                    Type::FIXED_LEN_BYTE_ARRAY,
                    vec![below(Scalar::Bytes(vec![0xFF, 0x00]), true)]
                ),
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
            rows(&intervals("day", Type::INT32, vec![point(Scalar::Int(2))]), "day", days),
            [false, true, false]
        );
    }

    #[test]
    fn an_unsigned_64_bit_pattern_compares_unsigned() {
        let big: ArrayRef = Arc::new(UInt64Array::from(vec![5, u64::MAX]));

        assert_eq!(
            rows(
                &intervals("u", Type::INT64, vec![below(Scalar::Int(-1), false)]),
                "u",
                big
            ),
            [true, false]
        );
    }

    #[test]
    fn bytes_compare_unsigned_and_prefixes_match_raw_bytes() {
        let strings: ArrayRef = Arc::new(StringArray::from(vec![Some("a"), Some("ź"), Some(""), None]));
        let prefix = |prefix: &str| FilterNode::StartsWith {
            path: vec!["s".to_string()],
            prefix: prefix.as_bytes().to_vec(),
        };

        assert_eq!(
            rows(
                &intervals("s", Type::BYTE_ARRAY, vec![below(Scalar::Bytes(b"z".to_vec()), false)]),
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
            rows(
                &FilterNode::StartsWith {
                    path: vec!["s".to_string()],
                    prefix: vec![0xC5]
                },
                "s",
                strings
            ),
            [false, true, false, false]
        );
    }

    #[test]
    fn a_uuid_compares_its_sixteen_bytes() {
        let ids: ArrayRef =
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![[1u8; 16], [2u8; 16]].into_iter()).unwrap());

        assert_eq!(
            rows(
                &intervals(
                    "id",
                    Type::FIXED_LEN_BYTE_ARRAY,
                    vec![point(Scalar::Bytes(vec![2; 16]))]
                ),
                "id",
                ids
            ),
            [false, true]
        );
    }

    #[test]
    fn an_absent_column_is_null_in_every_row() {
        let absent = |is_null| FilterNode::Absent { is_null };

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
                &intervals(
                    "i",
                    Type::INT64,
                    vec![below(Scalar::Int(2), false), point(Scalar::Int(5)), above]
                ),
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
        let null = |column: &str| FilterNode::IsNull {
            path: vec![column.to_string()],
        };
        let predicates = PlanPredicate::conjuncts(
            &FilterNode::And(vec![
                null("s"),
                FilterNode::And(vec![null("i"), FilterNode::Or(vec![null("f"), null("i")])]),
            ]),
            &schema,
            &descr,
        )
        .unwrap();

        assert_eq!(
            predicates.iter().map(PlanPredicate::leaves).collect::<Vec<_>>(),
            [&[2][..], &[0], &[1, 0]]
        );
        assert!(PlanPredicate::conjuncts(&FilterNode::And(vec![]), &schema, &descr)
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
        let cases: Vec<(FilterNode, &str, ArrayRef, Vec<bool>)> = vec![
            (
                intervals("u32", Type::INT32, vec![below(Scalar::Int(-1), false)]),
                "u32",
                Arc::new(UInt32Array::from(vec![5, u32::MAX])),
                vec![true, false],
            ),
            (
                not(intervals("f32", Type::FLOAT, vec![below(Scalar::Float(5.0), false)])),
                "f32",
                Arc::new(Float32Array::from(vec![f32::NAN, 3.0, 5.0])),
                vec![true, false, true],
            ),
            (
                intervals("t", Type::INT32, vec![point(Scalar::Int(2_000))]),
                "t",
                Arc::new(Time32MillisecondArray::from(vec![1_000, 2_000])),
                vec![false, true],
            ),
            (
                intervals(
                    "d",
                    Type::FIXED_LEN_BYTE_ARRAY,
                    vec![below(Scalar::Bytes(vec![0x04, 0xD2]), false)],
                ),
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
                &intervals("day", Type::INT32, vec![point(Scalar::Int(2))]),
                &arrow_schema::Schema::new(vec![day]),
                &descr,
                Arc::new(Date64Array::from(vec![86_400_000, 172_800_000, 172_800_001]))
            ),
            [false, true, false]
        );
        assert_eq!(
            kept(
                &intervals("seconds", Type::INT64, vec![below(Scalar::Int(2), true)]),
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
        let city = FilterNode::Intervals {
            path: vec!["address".to_string(), "city".to_string()],
            physical: Type::BYTE_ARRAY,
            intervals: vec![point(Scalar::Bytes(b"a".to_vec()))],
        };
        let null = FilterNode::IsNull {
            path: vec!["address".to_string(), "city".to_string()],
        };

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
                &intervals("b", Type::BOOLEAN, vec![below(Scalar::Bool(true), false)]),
                &schema,
                &descr,
                flags
            ),
            [true, false]
        );
    }

    #[test]
    fn a_bound_of_another_kind_than_the_column_is_refused() {
        let descr = descr();
        let schema = parquet_to_arrow_schema(&descr, None).unwrap();

        assert!(matches!(
            PlanPredicate::conjuncts(&intervals("i", Type::INT64, vec![point(Scalar::Bytes(b"1".to_vec()))]), &schema, &descr),
            Err(Error::Filter(message)) if message == "bounds of \"i\" are not Int64 values"
        ));
        assert!(matches!(
            PlanPredicate::conjuncts(&intervals("i", Type::INT32, vec![point(Scalar::Int(1))]), &schema, &descr),
            Err(Error::Plan(message)) if message == "Row filter column \"i\" is INT64, not INT32"
        ));
        assert!(matches!(
            PlanPredicate::conjuncts(&FilterNode::IsNull { path: vec!["missing".to_string()] }, &schema, &descr),
            Err(Error::Plan(message)) if message == "Row filter column \"missing\" is not a leaf of the file"
        ));
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
    fn the_converter_shape_parses() {
        let leaf = map(vec![
            ("leaf", bytes("intervals")),
            ("path", Raw::List(vec![bytes("address"), bytes("city")])),
            ("physical", bytes("BYTE_ARRAY")),
            (
                "intervals",
                Raw::List(vec![Raw::List(vec![
                    bytes("a"),
                    Raw::Bool(true),
                    Raw::Null,
                    Raw::Bool(false),
                ])]),
            ),
        ]);

        assert_eq!(
            FilterNode::parse(&map(vec![(
                "and",
                Raw::List(vec![
                    map(vec![(
                        "not",
                        map(vec![("leaf", bytes("absent")), ("is_null", Raw::Bool(true))])
                    )]),
                    map(vec![("or", Raw::List(vec![leaf]))]),
                    map(vec![("leaf", bytes("is_null")), ("path", Raw::List(vec![bytes("i")]))]),
                    map(vec![
                        ("leaf", bytes("starts_with")),
                        ("path", Raw::List(vec![bytes("s")])),
                        ("prefix", bytes("p"))
                    ]),
                    map(vec![("and", Raw::List(vec![]))]),
                ])
            )]))
            .unwrap(),
            FilterNode::And(vec![
                FilterNode::Not(Box::new(FilterNode::Absent { is_null: true })),
                FilterNode::Or(vec![FilterNode::Intervals {
                    path: vec!["address".to_string(), "city".to_string()],
                    physical: Type::BYTE_ARRAY,
                    intervals: vec![Interval {
                        lower: Some(Scalar::Bytes(b"a".to_vec())),
                        lower_inclusive: true,
                        upper: None,
                        upper_inclusive: false,
                    }],
                }]),
                FilterNode::IsNull {
                    path: vec!["i".to_string()]
                },
                FilterNode::StartsWith {
                    path: vec!["s".to_string()],
                    prefix: b"p".to_vec()
                },
                FilterNode::And(vec![]),
            ])
        );
    }

    #[test]
    fn an_unknown_leaf_or_a_missing_key_is_refused() {
        assert!(matches!(
            FilterNode::parse(&map(vec![("xor", Raw::List(vec![]))])),
            Err(Error::Filter(message)) if message == "missing key \"leaf\""
        ));
        assert!(matches!(
            FilterNode::parse(&map(vec![("leaf", bytes("between"))])),
            Err(Error::Filter(message)) if message.starts_with("unknown leaf")
        ));
        assert!(matches!(
            FilterNode::parse(&map(vec![("leaf", bytes("is_null"))])),
            Err(Error::Filter(message)) if message == "missing key \"path\""
        ));
        assert!(matches!(
            FilterNode::parse(&map(vec![("leaf", bytes("absent"))])),
            Err(Error::Filter(message)) if message == "missing key \"is_null\""
        ));
        assert!(matches!(
            FilterNode::parse(&map(vec![
                ("leaf", bytes("intervals")),
                ("path", Raw::List(vec![bytes("f")])),
                ("physical", bytes("DOUBLE")),
                ("intervals", Raw::List(vec![Raw::List(vec![Raw::Float(f64::NAN), Raw::Bool(true), Raw::Null, Raw::Bool(false)])])),
            ])),
            Err(Error::Filter(message)) if message == "a bound must not be NaN"
        ));
    }
}
