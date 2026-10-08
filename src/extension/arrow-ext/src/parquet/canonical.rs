//! The cast table between what arrow-rs reads from / writes to Parquet and the canonical arrays: one arrow type per
//! storage (the layout flow_php's columns store), keyed on the arrow `DataType` alone. Temporal and decimal
//! casts are this module's own - `arrow_cast` rounds toward zero and nulls out overflows.

use std::sync::Arc;

use arrow_array::cast::AsArray;
use arrow_array::types::{
    Date32Type, Date64Type, Decimal128Type, Decimal256Type, DecimalType, DurationMicrosecondType, Float32Type,
    Float64Type, GenericBinaryType, Int16Type, Int32Type, Int64Type, Int8Type, Time32MillisecondType, Time32SecondType,
    Time64NanosecondType, TimestampMicrosecondType, TimestampMillisecondType, TimestampNanosecondType,
    TimestampSecondType, UInt16Type, UInt32Type, UInt64Type, UInt8Type,
};
use arrow_array::{
    make_array, Array, ArrayRef, ArrowPrimitiveType, BinaryArray, FixedSizeBinaryArray, Float64Array, GenericByteArray,
    LargeBinaryArray, ListArray, MapArray, PrimitiveArray, StructArray,
};
use arrow_buffer::{OffsetBuffer, ScalarBuffer};
use arrow_schema::{DataType, Field, FieldRef, Fields, TimeUnit};

use crate::parquet::error::{parent_row, Error};

const UTC: &str = "UTC";

fn unsupported(data_type: &DataType) -> Error {
    Error::Unsupported {
        column: String::new(),
        parquet: data_type.to_string(),
    }
}

fn timestamp() -> DataType {
    DataType::Timestamp(TimeUnit::Microsecond, Some(UTC.into()))
}

fn item(data_type: DataType) -> FieldRef {
    Arc::new(Field::new("item", data_type, true))
}

fn entries(key: DataType, value: DataType) -> FieldRef {
    Arc::new(Field::new(
        "entries",
        DataType::Struct(Fields::from(vec![
            Field::new("key", key, false),
            Field::new("value", value, true),
        ])),
        false,
    ))
}

/// The canonical type `canonical()` turns an array of `field`'s type into.
// Must equal flow-batch-frame's kind::data_type() (flow-php-ext), nested names and nullability included - pinned by flow-php-ext phpt 091.
pub fn canonical_type(field: &Field) -> Result<DataType, Error> {
    canonical_data_type(field.data_type()).map_err(|e| e.in_column(field.name()))
}

fn canonical_data_type(data_type: &DataType) -> Result<DataType, Error> {
    Ok(match data_type {
        DataType::Null
        | DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Date32
        | DataType::FixedSizeBinary(16) => data_type.clone(),
        DataType::Int8
        | DataType::Int16
        | DataType::Int32
        | DataType::UInt8
        | DataType::UInt16
        | DataType::UInt32
        | DataType::UInt64 => DataType::Int64,
        DataType::Float32 | DataType::Decimal128(_, _) | DataType::Decimal256(_, _) => DataType::Float64,
        DataType::Utf8
        | DataType::Binary
        | DataType::LargeUtf8
        | DataType::LargeBinary
        | DataType::FixedSizeBinary(_) => DataType::LargeBinary,
        DataType::Timestamp(_, _) => timestamp(),
        DataType::Time64(TimeUnit::Microsecond | TimeUnit::Nanosecond)
        | DataType::Time32(TimeUnit::Millisecond | TimeUnit::Second) => DataType::Duration(TimeUnit::Microsecond),
        DataType::Date64 => DataType::Date32,
        DataType::List(element) | DataType::LargeList(element) => {
            DataType::List(item(canonical_data_type(element.data_type())?))
        }
        DataType::Map(field, _) => {
            let (key, value) = map_fields(field)?;

            DataType::Map(
                entries(
                    canonical_data_type(key.data_type())?,
                    canonical_data_type(value.data_type())?,
                ),
                false,
            )
        }
        DataType::Struct(fields) => DataType::Struct(struct_fields(fields)?),
        other => return Err(unsupported(other)),
    })
}

fn map_fields(entries: &FieldRef) -> Result<(&FieldRef, &FieldRef), Error> {
    match entries.data_type() {
        DataType::Struct(fields) if fields.len() == 2 => Ok((&fields[0], &fields[1])),
        other => Err(unsupported(other)),
    }
}

fn struct_fields(fields: &Fields) -> Result<Fields, Error> {
    fields
        .iter()
        .map(|field| Ok(Field::new(field.name(), canonical_data_type(field.data_type())?, true)))
        .collect()
}

/// `array` (of `field`'s type, as arrow-rs read it) as its canonical array; refusals name `field`.
pub fn canonical(field: &Field, array: &ArrayRef) -> Result<ArrayRef, Error> {
    cast(array).map_err(|e| e.in_column(field.name()))
}

fn widened<T: ArrowPrimitiveType>(array: &ArrayRef) -> ArrayRef
where
    T::Native: Into<i64>,
{
    Arc::new(array.as_primitive::<T>().unary::<_, Int64Type>(Into::into))
}

/// Every valid value through `f`; the first `None` refuses its row.
fn checked<T: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    array: &ArrayRef,
    f: impl Fn(T::Native) -> Option<O::Native>,
) -> Result<PrimitiveArray<O>, Error> {
    let input = array.as_primitive::<T>();
    let mut values = Vec::with_capacity(input.len());

    for (row, value) in input.values().iter().enumerate() {
        values.push(if input.is_null(row) {
            O::Native::default()
        } else {
            f(*value).ok_or(Error::Overflow {
                column: String::new(),
                row,
            })?
        });
    }

    Ok(PrimitiveArray::new(ScalarBuffer::from(values), input.nulls().cloned()))
}

fn retagged(array: &ArrayRef, to: DataType) -> ArrayRef {
    make_array(unsafe { array.to_data().into_builder().data_type(to).build_unchecked() })
}

fn micros<T: ArrowPrimitiveType<Native = i64>>(
    array: &ArrayRef,
    f: impl Fn(i64) -> Option<i64>,
) -> Result<ArrayRef, Error> {
    Ok(Arc::new(
        checked::<T, TimestampMicrosecondType>(array, f)?.with_timezone(UTC),
    ))
}

fn decimals<T: DecimalType>(array: &ArrayRef, precision: u8, scale: i8) -> Result<ArrayRef, Error> {
    let input = array.as_primitive::<T>();
    let mut values = Vec::with_capacity(input.len());

    for (row, value) in input.values().iter().enumerate() {
        values.push(if input.is_null(row) {
            0.0
        } else {
            T::format_decimal(*value, precision, scale)
                .parse::<f64>()
                .map_err(|e| Error::Arrow(arrow_schema::ArrowError::ParseError(e.to_string())))?
        });
    }

    Ok(Arc::new(Float64Array::new(
        ScalarBuffer::from(values),
        input.nulls().cloned(),
    )))
}

/// Large offsets narrowed to i32 and rebased to 0, with the values they span.
fn narrowed(offsets: &[i64]) -> Result<OffsetBuffer<i32>, Error> {
    let first = offsets[0];
    let narrowed = offsets
        .iter()
        .enumerate()
        .map(|(i, offset)| {
            i32::try_from(offset - first).map_err(|_| Error::Overflow {
                column: String::new(),
                row: i.saturating_sub(1),
            })
        })
        .collect::<Result<Vec<_>, _>>()?;

    Ok(OffsetBuffer::new(ScalarBuffer::from(narrowed)))
}

/// LargeBinary narrowed to Binary for a writer column: past `i32::MAX` bytes in one write it is refused.
fn large_binary(array: &ArrayRef) -> Result<ArrayRef, Error> {
    let data = array.to_data();
    let offsets = data.buffers()[0].typed_data::<i64>();
    let offsets = &offsets[data.offset()..data.offset() + data.len() + 1];
    let values = data.buffers()[1].slice_with_length(offsets[0] as usize, (offsets[data.len()] - offsets[0]) as usize);

    Ok(Arc::new(GenericByteArray::<GenericBinaryType<i32>>::new(
        narrowed(offsets)?,
        values,
        data.nulls().cloned(),
    )))
}

/// Binary or Utf8 as LargeBinary over the same value buffer: the offsets widened to i64.
fn widened_binary(array: &ArrayRef) -> ArrayRef {
    let data = array.to_data();
    let offsets = &data.buffers()[0].typed_data::<i32>()[data.offset()..data.offset() + data.len() + 1];

    Arc::new(LargeBinaryArray::new(
        OffsetBuffer::new(ScalarBuffer::from(
            offsets.iter().map(|offset| i64::from(*offset)).collect::<Vec<i64>>(),
        )),
        data.buffers()[1].clone(),
        data.nulls().cloned(),
    ))
}

/// Fixed-size values as LargeBinary over the same value buffer: offsets `n·i`.
fn fixed_binary(array: &ArrayRef) -> ArrayRef {
    let fixed = array.as_fixed_size_binary();
    let size = fixed.value_size() as i64;
    let offsets = (0..=fixed.len() as i64).map(|row| row * size).collect::<Vec<i64>>();

    Arc::new(LargeBinaryArray::new(
        OffsetBuffer::new(ScalarBuffer::from(offsets)),
        fixed.values().clone(),
        fixed.nulls().cloned(),
    ))
}

fn cast(array: &ArrayRef) -> Result<ArrayRef, Error> {
    Ok(match array.data_type() {
        DataType::Null
        | DataType::Boolean
        | DataType::Int64
        | DataType::Float64
        | DataType::Date32
        | DataType::FixedSizeBinary(16) => Arc::clone(array),
        DataType::Timestamp(TimeUnit::Microsecond, Some(zone)) if zone.as_ref() == UTC => Arc::clone(array),
        DataType::Int8 => widened::<Int8Type>(array),
        DataType::Int16 => widened::<Int16Type>(array),
        DataType::Int32 => widened::<Int32Type>(array),
        DataType::UInt8 => widened::<UInt8Type>(array),
        DataType::UInt16 => widened::<UInt16Type>(array),
        DataType::UInt32 => widened::<UInt32Type>(array),
        DataType::UInt64 => Arc::new(checked::<UInt64Type, Int64Type>(array, |value| {
            i64::try_from(value).ok()
        })?),
        DataType::Float32 => Arc::new(array.as_primitive::<Float32Type>().unary::<_, Float64Type>(f64::from)),
        DataType::Utf8 | DataType::Binary => widened_binary(array),
        DataType::LargeUtf8 | DataType::LargeBinary => retagged(array, DataType::LargeBinary),
        DataType::FixedSizeBinary(_) => fixed_binary(array),
        DataType::Timestamp(TimeUnit::Microsecond, _) => retagged(array, timestamp()),
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            micros::<TimestampMillisecondType>(array, |ms| ms.checked_mul(1_000))?
        }
        DataType::Timestamp(TimeUnit::Second, _) => micros::<TimestampSecondType>(array, |s| s.checked_mul(1_000_000))?,
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            micros::<TimestampNanosecondType>(array, |ns| Some(ns.div_euclid(1_000)))?
        }
        DataType::Time64(TimeUnit::Microsecond) => retagged(array, DataType::Duration(TimeUnit::Microsecond)),
        DataType::Time64(TimeUnit::Nanosecond) => Arc::new(
            array
                .as_primitive::<Time64NanosecondType>()
                .unary::<_, DurationMicrosecondType>(|ns| ns.div_euclid(1_000)),
        ),
        DataType::Time32(TimeUnit::Millisecond) => Arc::new(
            array
                .as_primitive::<Time32MillisecondType>()
                .unary::<_, DurationMicrosecondType>(|ms| i64::from(ms) * 1_000),
        ),
        DataType::Time32(TimeUnit::Second) => Arc::new(
            array
                .as_primitive::<Time32SecondType>()
                .unary::<_, DurationMicrosecondType>(|s| i64::from(s) * 1_000_000),
        ),
        DataType::Date64 => Arc::new(checked::<Date64Type, Date32Type>(array, |ms| {
            i32::try_from(ms.div_euclid(86_400_000)).ok()
        })?),
        DataType::Decimal128(precision, scale) => decimals::<Decimal128Type>(array, *precision, *scale)?,
        DataType::Decimal256(precision, scale) => decimals::<Decimal256Type>(array, *precision, *scale)?,
        DataType::List(_) => {
            let list = array.as_list::<i32>();
            let values = cast(list.values()).map_err(|e| e.at_row(|child| parent_row(list.value_offsets(), child)))?;

            Arc::new(ListArray::try_new(
                item(values.data_type().clone()),
                list.offsets().clone(),
                values,
                list.nulls().cloned(),
            )?)
        }
        DataType::LargeList(_) => {
            let list = array.as_list::<i64>();
            let offsets = list.value_offsets();
            let (first, last) = (offsets[0] as usize, offsets[offsets.len() - 1] as usize);
            let values = cast(&list.values().slice(first, last - first))
                .map_err(|e| e.at_row(|child| parent_row(offsets, child + first)))?;

            Arc::new(ListArray::try_new(
                item(values.data_type().clone()),
                narrowed(offsets)?,
                values,
                list.nulls().cloned(),
            )?)
        }
        DataType::Map(field, _) => {
            map_fields(field)?;
            let map = array.as_map();
            let at_parent = |e: Error| e.at_row(|child| parent_row(map.value_offsets(), child));
            let keys = cast(map.keys()).map_err(at_parent)?;
            let values = cast(map.values()).map_err(at_parent)?;
            let field = entries(keys.data_type().clone(), values.data_type().clone());
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!("entries() builds a struct")
            };

            Arc::new(MapArray::try_new(
                Arc::clone(&field),
                map.offsets().clone(),
                StructArray::try_new_with_length(fields.clone(), vec![keys, values], None, map.entries().len())?,
                map.nulls().cloned(),
                false,
            )?)
        }
        DataType::Struct(fields) => {
            let structure = array.as_struct();
            let children = structure.columns().iter().map(cast).collect::<Result<Vec<_>, _>>()?;
            let fields = fields
                .iter()
                .zip(&children)
                .map(|(field, child)| Field::new(field.name(), child.data_type().clone(), true))
                .collect::<Fields>();

            Arc::new(StructArray::try_new_with_length(
                fields,
                children,
                structure.nulls().cloned(),
                structure.len(),
            )?)
        }
        other => return Err(unsupported(other)),
    })
}

/// A canonical array as the writer schema's `target` type: the inverse retags, UTF-8 validated where Parquet requires
/// it, nested children renamed to the target's fields. Refusals carry no column - the writer names it.
pub fn to_parquet(array: &ArrayRef, target: &DataType) -> Result<ArrayRef, Error> {
    if array.data_type() == target {
        return Ok(Arc::clone(array));
    }

    Ok(match (array.data_type(), target) {
        (DataType::LargeBinary, DataType::Binary) => large_binary(array)?,
        (DataType::LargeBinary, DataType::Utf8) => utf8(&large_binary(array)?)?,
        (DataType::Int64, DataType::Int8) => Arc::new(narrowed_int::<Int8Type>(array)?),
        (DataType::Int64, DataType::Int16) => Arc::new(narrowed_int::<Int16Type>(array)?),
        (DataType::Int64, DataType::Int32) => Arc::new(narrowed_int::<Int32Type>(array)?),
        (DataType::Int64, DataType::UInt8) => Arc::new(narrowed_int::<UInt8Type>(array)?),
        (DataType::Int64, DataType::UInt16) => Arc::new(narrowed_int::<UInt16Type>(array)?),
        (DataType::Int64, DataType::UInt32) => Arc::new(narrowed_int::<UInt32Type>(array)?),
        (DataType::Int64, DataType::UInt64) => Arc::new(narrowed_int::<UInt64Type>(array)?),
        (DataType::Float64, DataType::Float32) => Arc::new(
            array
                .as_primitive::<Float64Type>()
                .unary::<_, Float32Type>(|value| value as f32),
        ),
        (DataType::Float64, DataType::Decimal128(precision, scale)) => Arc::new(
            checked_values::<Float64Type, Decimal128Type>(array, |value| decimal_unscaled(value, *precision, *scale))?
                .with_precision_and_scale(*precision, *scale)?,
        ),
        (DataType::LargeBinary, DataType::FixedSizeBinary(size)) => fixed_size(&large_binary(array)?, *size)?,
        (DataType::Duration(TimeUnit::Microsecond), DataType::Time64(TimeUnit::Microsecond)) => {
            retagged(array, target.clone())
        }
        (DataType::Duration(TimeUnit::Microsecond), DataType::Time32(TimeUnit::Millisecond)) => {
            Arc::new(checked::<DurationMicrosecondType, Time32MillisecondType>(
                array,
                |us| i32::try_from(us.div_euclid(1_000)).ok(),
            )?)
        }
        (DataType::Duration(TimeUnit::Microsecond), DataType::Time64(TimeUnit::Nanosecond)) => {
            Arc::new(checked::<DurationMicrosecondType, Time64NanosecondType>(array, |us| {
                us.checked_mul(1_000)
            })?)
        }
        (DataType::Timestamp(TimeUnit::Microsecond, Some(zone)), DataType::Timestamp(TimeUnit::Millisecond, to))
            if zone.as_ref() == UTC =>
        {
            Arc::new(
                checked::<TimestampMicrosecondType, TimestampMillisecondType>(array, |us| Some(us.div_euclid(1_000)))?
                    .with_timezone_opt(to.clone()),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, Some(zone)), DataType::Timestamp(TimeUnit::Nanosecond, to))
            if zone.as_ref() == UTC =>
        {
            Arc::new(
                checked::<TimestampMicrosecondType, TimestampNanosecondType>(array, |us| us.checked_mul(1_000))?
                    .with_timezone_opt(to.clone()),
            )
        }
        (DataType::Timestamp(TimeUnit::Microsecond, Some(zone)), DataType::Timestamp(TimeUnit::Microsecond, _))
            if zone.as_ref() == UTC =>
        {
            retagged(array, target.clone())
        }
        (DataType::List(_), DataType::List(element)) => {
            let list = array.as_list::<i32>();

            Arc::new(ListArray::try_new(
                Arc::clone(element),
                list.offsets().clone(),
                to_parquet(list.values(), element.data_type())
                    .map_err(|e| e.at_row(|child| parent_row(list.value_offsets(), child)))?,
                list.nulls().cloned(),
            )?)
        }
        (DataType::Map(_, _), DataType::Map(field, sorted)) => {
            let (key, value) = map_fields(field)?;
            let DataType::Struct(fields) = field.data_type() else {
                unreachable!("map_fields() matched a struct")
            };
            let map = array.as_map();
            let at_parent = |e: Error| e.at_row(|child| parent_row(map.value_offsets(), child));

            Arc::new(MapArray::try_new(
                Arc::clone(field),
                map.offsets().clone(),
                StructArray::try_new_with_length(
                    fields.clone(),
                    vec![
                        to_parquet(map.keys(), key.data_type()).map_err(at_parent)?,
                        to_parquet(map.values(), value.data_type()).map_err(at_parent)?,
                    ],
                    None,
                    map.entries().len(),
                )?,
                map.nulls().cloned(),
                *sorted,
            )?)
        }
        (DataType::Struct(from), DataType::Struct(fields)) if from.len() == fields.len() => {
            let structure = array.as_struct();

            Arc::new(StructArray::try_new_with_length(
                fields.clone(),
                structure
                    .columns()
                    .iter()
                    .zip(fields.iter())
                    .map(|(child, field)| to_parquet(child, field.data_type()))
                    .collect::<Result<Vec<_>, _>>()?,
                structure.nulls().cloned(),
                structure.len(),
            )?)
        }
        _ => return Err(unsupported(target)),
    })
}

/// Int64 values in `T`'s range; the first one outside refuses its row.
fn narrowed_int<T: ArrowPrimitiveType>(array: &ArrayRef) -> Result<PrimitiveArray<T>, Error>
where
    T::Native: TryFrom<i64>,
{
    checked::<Int64Type, T>(array, |value| T::Native::try_from(value).ok())
}

/// Every valid value through `f`, which refuses a row with the error it returns.
fn checked_values<T: ArrowPrimitiveType, O: ArrowPrimitiveType>(
    array: &ArrayRef,
    f: impl Fn(T::Native) -> Result<O::Native, Error>,
) -> Result<PrimitiveArray<O>, Error> {
    let input = array.as_primitive::<T>();
    let mut values = Vec::with_capacity(input.len());

    for (row, value) in input.values().iter().enumerate() {
        values.push(if input.is_null(row) {
            O::Native::default()
        } else {
            f(*value).map_err(|error| error.at_row(|_| row))?
        });
    }

    Ok(PrimitiveArray::new(ScalarBuffer::from(values), input.nulls().cloned()))
}

/// `Flow\Parquet\Binary\decimal_unscaled()`: the float's shortest round-trip digits (Rust `{}` prints the digits PHP's
/// `var_export()` does, never an exponent), rounded half away from zero at `scale`; more than `precision` digits
/// overflow.
fn decimal_unscaled(value: f64, precision: u8, scale: i8) -> Result<i128, Error> {
    if !value.is_finite() {
        return Err(Error::Value {
            column: String::new(),
            row: 0,
            expected: "a finite number",
            got: if value.is_nan() {
                "NAN".to_string()
            } else if value > 0.0 {
                "INF".to_string()
            } else {
                "-INF".to_string()
            },
        });
    }

    let overflow = || Error::Overflow {
        column: String::new(),
        row: 0,
    };
    let text = format!("{}", value.abs());
    let (integer, fraction) = text.split_once('.').unwrap_or((&text, ""));
    let scale = scale.max(0) as usize;
    let kept = fraction.get(..scale.min(fraction.len())).unwrap_or("");
    let mut digits = format!("{integer}{kept}{}", "0".repeat(scale - kept.len()));
    let round_up = fraction.as_bytes().get(scale).is_some_and(|digit| *digit >= b'5');
    digits = digits.trim_start_matches('0').to_string();

    if digits.len() > usize::from(precision) || digits.len() > 38 {
        return Err(overflow());
    }

    let mut unscaled = if digits.is_empty() {
        0
    } else {
        digits.parse::<i128>().map_err(|_| overflow())?
    };

    if round_up {
        unscaled += 1;

        if unscaled.to_string().len() > usize::from(precision) {
            return Err(overflow());
        }
    }

    Ok(if value.is_sign_negative() { -unscaled } else { unscaled })
}

/// Binary values of exactly `size` bytes as fixed-size binary; the first of another length refuses its row.
fn fixed_size(array: &ArrayRef, size: i32) -> Result<ArrayRef, Error> {
    let binary = array.as_binary::<i32>();
    let width = size as usize;
    let mut values = vec![0u8; binary.len() * width];

    for row in 0..binary.len() {
        if binary.is_null(row) {
            continue;
        }

        let value = binary.value(row);

        if value.len() != width {
            return Err(Error::Length {
                column: String::new(),
                row,
                expected: width,
            });
        }

        values[row * width..(row + 1) * width].copy_from_slice(value);
    }

    Ok(Arc::new(FixedSizeBinaryArray::new(
        size,
        values.into(),
        binary.nulls().cloned(),
    )))
}

/// Binary as Utf8 over the same buffers once every value is valid UTF-8; else the first row that is not.
fn utf8(array: &ArrayRef) -> Result<ArrayRef, Error> {
    match array.to_data().into_builder().data_type(DataType::Utf8).build() {
        Ok(data) => Ok(make_array(data)),
        Err(error) => {
            let binary: &BinaryArray = array.as_binary::<i32>();

            Err((0..binary.len())
                .find(|row| binary.is_valid(*row) && std::str::from_utf8(binary.value(*row)).is_err())
                .map_or(Error::Arrow(error), |row| Error::InvalidUtf8 {
                    column: String::new(),
                    row,
                }))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::builder::{Int32Builder, ListBuilder, MapBuilder, StringBuilder};
    use arrow_array::cast::AsArray;
    use arrow_array::types::{
        Date32Type, Decimal128Type, DurationMicrosecondType, Float32Type, Float64Type, Int64Type, Int8Type,
        Time32MillisecondType, Time64MicrosecondType, Time64NanosecondType, TimestampMicrosecondType,
        TimestampMillisecondType, TimestampNanosecondType, UInt64Type,
    };
    use arrow_array::{
        Array, ArrayRef, BinaryArray, BooleanArray, Date32Array, Date64Array, Decimal128Array, Decimal256Array,
        DurationMicrosecondArray, FixedSizeBinaryArray, Float32Array, Float64Array, Int16Array, Int32Array, Int64Array,
        Int8Array, LargeBinaryArray, LargeListArray, LargeStringArray, ListArray, StringArray, StructArray,
        Time32MillisecondArray, Time32SecondArray, Time64MicrosecondArray, Time64NanosecondArray,
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray, TimestampSecondArray,
        UInt16Array, UInt32Array, UInt64Array, UInt8Array,
    };
    use arrow_buffer::i256;
    use arrow_schema::{DataType, Field, Fields, TimeUnit};

    use super::{canonical, canonical_type, to_parquet};
    use crate::parquet::error::Error;

    fn read(array: ArrayRef) -> ArrayRef {
        let field = Field::new("column", array.data_type().clone(), true);
        let canonical = canonical(&field, &array).unwrap();

        assert_eq!(canonical.data_type(), &canonical_type(&field).unwrap());

        canonical
    }

    fn refusal(array: ArrayRef) -> Error {
        canonical(&Field::new("column", array.data_type().clone(), true), &array).unwrap_err()
    }

    fn int64(array: &ArrayRef) -> Vec<Option<i64>> {
        array.as_primitive::<Int64Type>().iter().collect()
    }

    /// The canonical list layout: a nullable `item` child.
    fn list_of(element: DataType) -> DataType {
        DataType::List(Arc::new(Field::new("item", element, true)))
    }

    /// The canonical map layout: `entries` of a non-null `key` and a nullable `value`.
    fn map_of(key: DataType, value: DataType) -> DataType {
        DataType::Map(
            Arc::new(Field::new(
                "entries",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", key, false),
                    Field::new("value", value, true),
                ])),
                false,
            )),
            false,
        )
    }

    #[test]
    fn canonical_types_are_kept() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(BooleanArray::from(vec![Some(true), None])),
            Arc::new(Int64Array::from(vec![Some(1), None])),
            Arc::new(Float64Array::from(vec![Some(1.5), None])),
            Arc::new(Date32Array::from(vec![Some(-1), None])),
            Arc::new(FixedSizeBinaryArray::try_from_iter(vec![[7u8; 16]].into_iter()).unwrap()),
            Arc::new(TimestampMicrosecondArray::from(vec![Some(-1), None]).with_timezone("UTC")),
        ];

        for array in arrays {
            assert!(Arc::ptr_eq(&read(Arc::clone(&array)), &array));
        }
    }

    #[test]
    fn integers_widen_to_int64() {
        let expected = vec![Some(-3), None, Some(7)];

        assert_eq!(
            int64(&read(Arc::new(Int8Array::from(vec![Some(-3), None, Some(7)])))),
            expected
        );
        assert_eq!(
            int64(&read(Arc::new(Int16Array::from(vec![Some(-3), None, Some(7)])))),
            expected
        );
        assert_eq!(
            int64(&read(Arc::new(Int32Array::from(vec![Some(-3), None, Some(7)])))),
            expected
        );
        assert_eq!(
            int64(&read(Arc::new(UInt8Array::from(vec![Some(255), None])))),
            vec![Some(255), None]
        );
        assert_eq!(
            int64(&read(Arc::new(UInt16Array::from(vec![Some(65_535), None])))),
            vec![Some(65_535), None]
        );
        assert_eq!(
            int64(&read(Arc::new(UInt32Array::from(vec![Some(u32::MAX), None])))),
            vec![Some(i64::from(u32::MAX)), None]
        );
        assert_eq!(
            int64(&read(Arc::new(UInt64Array::from(vec![Some(i64::MAX as u64), None])))),
            vec![Some(i64::MAX), None]
        );
    }

    #[test]
    fn uint64_above_int64_is_refused_at_its_row() {
        let error = refusal(Arc::new(UInt64Array::from(vec![
            Some(1),
            None,
            Some(i64::MAX as u64 + 1),
        ])));

        assert!(matches!(error, Error::Overflow { column, row: 2 } if column == "column"));
    }

    #[test]
    fn a_null_slot_never_overflows() {
        let values = UInt64Array::new(vec![1, u64::MAX].into(), Some(vec![true, false].into()));

        assert_eq!(int64(&read(Arc::new(values))), vec![Some(1), None]);
    }

    #[test]
    fn float32_widens_to_float64() {
        let canonical = read(Arc::new(Float32Array::from(vec![Some(0.5), None])));

        assert_eq!(
            canonical.as_primitive::<Float64Type>().iter().collect::<Vec<_>>(),
            vec![Some(0.5), None]
        );
    }

    #[test]
    fn strings_and_binaries_are_large_binary() {
        let expected: Vec<Option<&[u8]>> = vec![Some(b"a"), None, Some(b"")];

        for array in [
            Arc::new(StringArray::from(vec![Some("a"), None, Some("")])) as ArrayRef,
            Arc::new(BinaryArray::from(expected.clone())),
            Arc::new(LargeStringArray::from(vec![Some("a"), None, Some("")])),
            Arc::new(LargeBinaryArray::from(expected.clone())),
        ] {
            let canonical = read(array);

            assert_eq!(canonical.as_binary::<i64>().iter().collect::<Vec<_>>(), expected);
        }
    }

    #[test]
    fn a_sliced_large_string_keeps_its_rows() {
        let array = LargeStringArray::from(vec!["skipped", "kept", "too"]).slice(1, 2);

        assert_eq!(
            read(Arc::new(array)).as_binary::<i64>().iter().collect::<Vec<_>>(),
            vec![Some(&b"kept"[..]), Some(&b"too"[..])]
        );
    }

    #[test]
    fn a_sliced_string_is_widened_to_its_rows() {
        let array = StringArray::from(vec!["skipped", "kept", "too"]).slice(1, 2);

        assert_eq!(
            read(Arc::new(array)).as_binary::<i64>().iter().collect::<Vec<_>>(),
            vec![Some(&b"kept"[..]), Some(&b"too"[..])]
        );
    }

    #[test]
    fn timestamps_are_utc_microseconds() {
        let micros = |array: ArrayRef| -> Vec<Option<i64>> {
            read(array).as_primitive::<TimestampMicrosecondType>().iter().collect()
        };

        assert_eq!(
            micros(Arc::new(TimestampMicrosecondArray::from(vec![Some(-7), None]))),
            vec![Some(-7), None]
        );
        assert_eq!(
            micros(Arc::new(
                TimestampMicrosecondArray::from(vec![Some(5)]).with_timezone("Europe/Warsaw")
            )),
            vec![Some(5)]
        );
        assert_eq!(
            micros(Arc::new(TimestampMillisecondArray::from(vec![Some(-2), None]))),
            vec![Some(-2_000), None]
        );
        assert_eq!(
            micros(Arc::new(TimestampSecondArray::from(vec![Some(3)]))),
            vec![Some(3_000_000)]
        );
        assert_eq!(
            micros(Arc::new(TimestampNanosecondArray::from(vec![
                Some(-1_500),
                Some(1_999),
                None
            ]))),
            vec![Some(-2), Some(1), None]
        );
    }

    #[test]
    fn a_timestamp_beyond_microseconds_is_refused() {
        let error = refusal(Arc::new(TimestampMillisecondArray::from(vec![Some(0), Some(i64::MAX)])));

        assert!(matches!(error, Error::Overflow { row: 1, .. }));
        assert!(matches!(
            refusal(Arc::new(TimestampSecondArray::from(vec![Some(i64::MIN)]))),
            Error::Overflow { row: 0, .. }
        ));
    }

    #[test]
    fn times_are_microsecond_durations() {
        let micros = |array: ArrayRef| -> Vec<Option<i64>> {
            read(array).as_primitive::<DurationMicrosecondType>().iter().collect()
        };

        assert_eq!(
            micros(Arc::new(Time64MicrosecondArray::from(vec![Some(5), None]))),
            vec![Some(5), None]
        );
        assert_eq!(
            micros(Arc::new(Time64NanosecondArray::from(vec![Some(1_999), None]))),
            vec![Some(1), None]
        );
        assert_eq!(
            micros(Arc::new(Time32MillisecondArray::from(vec![Some(2), None]))),
            vec![Some(2_000), None]
        );
        assert_eq!(
            micros(Arc::new(Time32SecondArray::from(vec![Some(3)]))),
            vec![Some(3_000_000)]
        );
    }

    #[test]
    fn date64_floors_to_days() {
        let days = read(Arc::new(Date64Array::from(vec![Some(-1), Some(86_400_000), None])));

        assert_eq!(
            days.as_primitive::<Date32Type>().iter().collect::<Vec<_>>(),
            vec![Some(-1), Some(1), None]
        );
        assert!(matches!(
            refusal(Arc::new(Date64Array::from(vec![Some(i64::MAX)]))),
            Error::Overflow { row: 0, .. }
        ));
    }

    #[test]
    fn decimals_are_floats_through_their_decimal_text() {
        let decimal128 = Decimal128Array::from(vec![Some(12_345), Some(-5), None])
            .with_precision_and_scale(9, 2)
            .unwrap();
        let decimal256 = Decimal256Array::from(vec![Some(i256::from_i128(10_000_000_001)), None])
            .with_precision_and_scale(50, 10)
            .unwrap();

        assert_eq!(
            read(Arc::new(decimal128))
                .as_primitive::<Float64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(123.45), Some(-0.05), None]
        );
        assert_eq!(
            read(Arc::new(decimal256))
                .as_primitive::<Float64Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1.0000000001), None]
        );
    }

    #[test]
    fn lists_are_renamed_to_the_kind_layout() {
        let mut builder =
            ListBuilder::new(Int32Builder::new()).with_field(Field::new("element", DataType::Int32, false));
        builder.append_value([Some(1), Some(2)]);
        builder.append_null();
        builder.append_value([Some(3)]);
        let canonical = read(Arc::new(builder.finish()));

        assert_eq!(canonical.data_type(), &list_of(DataType::Int64));
        assert_eq!(
            int64(canonical.as_list::<i32>().values()),
            vec![Some(1), Some(2), Some(3)]
        );
        assert!(canonical.is_null(1));
    }

    #[test]
    fn large_lists_narrow_their_offsets() {
        let array = LargeListArray::from_iter_primitive::<arrow_array::types::Int32Type, _, _>(vec![
            Some(vec![Some(9)]),
            Some(vec![Some(1), None]),
            None,
        ])
        .slice(1, 2);
        let canonical = read(Arc::new(array));
        let list = canonical.as_list::<i32>();

        assert_eq!(canonical.data_type(), &list_of(DataType::Int64));
        assert_eq!(list.value_offsets(), &[0, 2, 2]);
        assert_eq!(int64(list.values()), vec![Some(1), None]);
    }

    #[test]
    fn maps_are_renamed_to_the_kind_layout() {
        let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
        builder.keys().append_value("a");
        builder.values().append_value(1);
        builder.append(true).unwrap();
        builder.append(false).unwrap();
        let canonical = read(Arc::new(builder.finish()));

        assert_eq!(canonical.data_type(), &map_of(DataType::LargeBinary, DataType::Int64));
        assert_eq!(int64(canonical.as_map().values()), vec![Some(1)]);
        assert!(canonical.is_null(1));
    }

    #[test]
    fn structs_are_relabelled_nullable() {
        let array = StructArray::from(vec![
            (
                Arc::new(Field::new("id", DataType::Int32, false)),
                Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef,
            ),
            (
                Arc::new(Field::new("name", DataType::Utf8, false)),
                Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
            ),
        ]);
        let canonical = read(Arc::new(array));

        assert_eq!(
            canonical.data_type(),
            &DataType::Struct(Fields::from(vec![
                Field::new("id", DataType::Int64, true),
                Field::new("name", DataType::LargeBinary, true),
            ]))
        );
        assert_eq!(int64(canonical.as_struct().column(0)), vec![Some(1), Some(2)]);
    }

    #[test]
    fn a_nested_overflow_is_refused_at_its_parent_row() {
        let values = Arc::new(UInt64Array::from(vec![1, 2, u64::MAX]));
        let list = ListArray::new(
            Arc::new(Field::new("element", DataType::UInt64, false)),
            arrow_buffer::OffsetBuffer::new(vec![0, 2, 3].into()),
            values,
            None,
        );

        assert!(matches!(refusal(Arc::new(list)), Error::Overflow { row: 1, .. }));
    }

    #[test]
    fn a_list_of_nulls_is_kept() {
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("element", DataType::Null, true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 0, 2].into()),
            Arc::new(arrow_array::NullArray::new(2)),
            None,
        ));
        let canonical = read(list);

        assert_eq!(canonical.as_list::<i32>().value_offsets(), &[0, 0, 2]);
        assert_eq!(canonical.as_list::<i32>().values().data_type(), &DataType::Null);
    }

    #[test]
    fn unsupported_types_are_refused_naming_the_type() {
        let interval = DataType::Interval(arrow_schema::IntervalUnit::DayTime);
        let field = Field::new(
            "intervals",
            DataType::List(Arc::new(Field::new("element", interval, true))),
            true,
        );

        assert!(matches!(
            canonical_type(&field),
            Err(Error::Unsupported { column, parquet }) if column == "intervals" && parquet == "Interval(DayTime)"
        ));
        assert!(matches!(
            refusal(Arc::new(arrow_array::DurationSecondArray::from(vec![1]))),
            Error::Unsupported { .. }
        ));
    }

    #[test]
    fn fixed_size_binaries_of_another_size_than_16_are_large_binary() {
        let array = FixedSizeBinaryArray::try_from_sparse_iter_with_size(
            vec![Some(*b"abcd"), None, Some(*b"wxyz"), Some(*b"skip")].into_iter(),
            4,
        )
        .unwrap()
        .slice(0, 3);
        let canonical = read(Arc::new(array));

        assert_eq!(
            canonical.as_binary::<i64>().iter().collect::<Vec<_>>(),
            vec![Some(&b"abcd"[..]), None, Some(&b"wxyz"[..])]
        );
    }

    #[test]
    fn to_parquet_narrows_integers_in_range() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![Some(-128), None, Some(127)]));

        assert_eq!(
            to_parquet(&array, &DataType::Int8)
                .unwrap()
                .as_primitive::<Int8Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(-128), None, Some(127)]
        );

        let unsigned: ArrayRef = Arc::new(Int64Array::from(vec![Some(4_000_000_000), Some(i64::MAX)]));

        assert_eq!(
            to_parquet(&unsigned, &DataType::UInt64)
                .unwrap()
                .as_primitive::<UInt64Type>()
                .values()
                .to_vec(),
            vec![4_000_000_000, i64::MAX as u64]
        );

        for target in [
            DataType::Int16,
            DataType::Int32,
            DataType::UInt8,
            DataType::UInt16,
            DataType::UInt32,
        ] {
            assert_eq!(to_parquet(&array.slice(1, 1), &target).unwrap().data_type(), &target);
        }
    }

    #[test]
    fn to_parquet_refuses_integers_out_of_the_target_range_at_their_row() {
        let cases = [
            (DataType::Int8, vec![1, 128]),
            (DataType::Int16, vec![1, -32_769]),
            (DataType::Int32, vec![1, 2_147_483_648]),
            (DataType::UInt8, vec![1, 256]),
            (DataType::UInt16, vec![1, -1]),
            (DataType::UInt32, vec![1, 4_294_967_296]),
            (DataType::UInt64, vec![1, -1]),
        ];

        for (target, values) in cases {
            let array: ArrayRef = Arc::new(Int64Array::from(values));

            assert!(
                matches!(to_parquet(&array, &target), Err(Error::Overflow { row: 1, .. })),
                "{target}"
            );
        }
    }

    #[test]
    fn to_parquet_narrows_floats() {
        let array: ArrayRef = Arc::new(Float64Array::from(vec![Some(0.5), None]));

        assert_eq!(
            to_parquet(&array, &DataType::Float32)
                .unwrap()
                .as_primitive::<Float32Type>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(0.5), None]
        );
    }

    fn unscaled(values: Vec<Option<f64>>, precision: u8, scale: i8) -> Result<Vec<Option<i128>>, Error> {
        let array: ArrayRef = Arc::new(Float64Array::from(values));

        to_parquet(&array, &DataType::Decimal128(precision, scale))
            .map(|decimal| decimal.as_primitive::<Decimal128Type>().iter().collect())
    }

    #[test]
    fn to_parquet_writes_decimals_from_the_shortest_digits_half_away_from_zero() {
        assert_eq!(
            unscaled(
                vec![
                    Some(123.45),
                    Some(-0.125),
                    Some(0.125),
                    Some(0.1249),
                    None,
                    Some(-0.001)
                ],
                9,
                2
            )
            .unwrap(),
            vec![Some(12_345), Some(-13), Some(13), Some(12), None, Some(0)]
        );
        assert_eq!(unscaled(vec![Some(1e20)], 38, 2).unwrap(), vec![Some(10i128.pow(22))]);
        assert_eq!(
            unscaled(vec![Some(0.1 + 0.2)], 20, 17).unwrap(),
            vec![Some(30_000_000_000_000_004)]
        );
        assert_eq!(unscaled(vec![Some(-5.0)], 1, -1).unwrap(), vec![Some(-5)]);
    }

    #[test]
    fn to_parquet_rounds_decimals_of_binary_fractions_half_away_from_zero() {
        assert_eq!(
            unscaled(
                vec![
                    Some(0.125),
                    Some(2.675),
                    Some(-0.125),
                    Some(1.005),
                    Some(9.995),
                    Some(-99.995)
                ],
                9,
                2
            )
            .unwrap(),
            vec![Some(13), Some(268), Some(-13), Some(101), Some(1_000), Some(-10_000)]
        );
    }

    #[test]
    fn to_parquet_refuses_decimals_beyond_their_precision() {
        assert!(matches!(
            unscaled(vec![Some(1.0), Some(100.0)], 4, 2),
            Err(Error::Overflow { row: 1, .. })
        ));
        assert!(matches!(
            unscaled(vec![Some(99.995)], 4, 2),
            Err(Error::Overflow { row: 0, .. })
        ));
        assert!(matches!(
            unscaled(vec![Some(1e300)], 38, 0),
            Err(Error::Overflow { row: 0, .. })
        ));
    }

    #[test]
    fn to_parquet_refuses_decimals_that_are_not_finite() {
        for (value, got) in [(f64::NAN, "NAN"), (f64::INFINITY, "INF"), (f64::NEG_INFINITY, "-INF")] {
            assert!(matches!(
                unscaled(vec![Some(1.0), Some(value)], 9, 2),
                Err(Error::Value { row: 1, expected, got: text, .. }) if expected == "a finite number" && text == got
            ));
        }
    }

    #[test]
    fn to_parquet_writes_fixed_size_binaries_of_their_length() {
        let array: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"abcd"[..]), None]));
        let fixed = to_parquet(&array, &DataType::FixedSizeBinary(4)).unwrap();

        assert_eq!(
            fixed.as_fixed_size_binary().iter().collect::<Vec<_>>(),
            vec![Some(&b"abcd"[..]), None]
        );
        assert!(matches!(
            to_parquet(&array, &DataType::FixedSizeBinary(16)),
            Err(Error::Length {
                row: 0,
                expected: 16,
                ..
            })
        ));
    }

    #[test]
    fn to_parquet_writes_times_in_milliseconds_and_nanoseconds() {
        let time: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![Some(1_999), Some(-1), None]));

        assert_eq!(
            to_parquet(&time, &DataType::Time32(TimeUnit::Millisecond))
                .unwrap()
                .as_primitive::<Time32MillisecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(-1), None]
        );
        assert_eq!(
            to_parquet(&time, &DataType::Time64(TimeUnit::Nanosecond))
                .unwrap()
                .as_primitive::<Time64NanosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1_999_000), Some(-1_000), None]
        );

        let long: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![0, i64::MAX]));

        assert!(matches!(
            to_parquet(&long, &DataType::Time32(TimeUnit::Millisecond)),
            Err(Error::Overflow { row: 1, .. })
        ));
        assert!(matches!(
            to_parquet(&long, &DataType::Time64(TimeUnit::Nanosecond)),
            Err(Error::Overflow { row: 1, .. })
        ));
    }

    #[test]
    fn to_parquet_writes_timestamps_in_milliseconds_and_nanoseconds() {
        let timestamp: ArrayRef =
            Arc::new(TimestampMicrosecondArray::from(vec![Some(1_999), Some(-1), None]).with_timezone("UTC"));
        let millis = to_parquet(&timestamp, &DataType::Timestamp(TimeUnit::Millisecond, None)).unwrap();
        let nanos = to_parquet(
            &timestamp,
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into())),
        )
        .unwrap();

        assert_eq!(millis.data_type(), &DataType::Timestamp(TimeUnit::Millisecond, None));
        assert_eq!(
            millis
                .as_primitive::<TimestampMillisecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1), Some(-1), None]
        );
        assert_eq!(
            nanos.data_type(),
            &DataType::Timestamp(TimeUnit::Nanosecond, Some("UTC".into()))
        );
        assert_eq!(
            nanos
                .as_primitive::<TimestampNanosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(1_999_000), Some(-1_000), None]
        );

        let far: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![0, i64::MAX]).with_timezone("UTC"));

        assert!(matches!(
            to_parquet(&far, &DataType::Timestamp(TimeUnit::Nanosecond, None)),
            Err(Error::Overflow { row: 1, .. })
        ));
    }

    #[test]
    fn to_parquet_keeps_equal_types() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));

        assert!(Arc::ptr_eq(&to_parquet(&array, &DataType::Int64).unwrap(), &array));
    }

    #[test]
    fn to_parquet_retags_temporal_types() {
        let time: ArrayRef = Arc::new(DurationMicrosecondArray::from(vec![Some(5), None]));
        let timestamp: ArrayRef = Arc::new(TimestampMicrosecondArray::from(vec![Some(-1)]).with_timezone("UTC"));

        assert_eq!(
            to_parquet(&time, &DataType::Time64(TimeUnit::Microsecond))
                .unwrap()
                .as_primitive::<Time64MicrosecondType>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(5), None]
        );
        assert_eq!(
            to_parquet(&timestamp, &DataType::Timestamp(TimeUnit::Microsecond, None))
                .unwrap()
                .data_type(),
            &DataType::Timestamp(TimeUnit::Microsecond, None)
        );
    }

    #[test]
    fn to_parquet_writes_binary_narrowed() {
        let binary: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"zazolc"[..]), None]));

        assert_eq!(
            to_parquet(&binary, &DataType::Binary)
                .unwrap()
                .as_binary::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some(&b"zazolc"[..]), None]
        );
    }

    #[test]
    fn to_parquet_writes_valid_utf8_as_strings() {
        let binary: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"zazolc"[..]), None]));

        assert_eq!(
            to_parquet(&binary, &DataType::Utf8)
                .unwrap()
                .as_string::<i32>()
                .iter()
                .collect::<Vec<_>>(),
            vec![Some("zazolc"), None]
        );
    }

    #[test]
    fn to_parquet_refuses_invalid_utf8_at_its_row() {
        let binary: ArrayRef = Arc::new(LargeBinaryArray::from(vec![
            Some(&b"ok"[..]),
            None,
            Some(&b"\xff\xfe"[..]),
        ]));

        assert!(matches!(
            to_parquet(&binary, &DataType::Utf8),
            Err(Error::InvalidUtf8 { row: 2, .. })
        ));
    }

    #[test]
    fn to_parquet_refuses_invalid_utf8_at_its_parent_row() {
        let list: ArrayRef = Arc::new(ListArray::new(
            Arc::new(Field::new("item", DataType::LargeBinary, true)),
            arrow_buffer::OffsetBuffer::new(vec![0, 1, 3].into()),
            Arc::new(LargeBinaryArray::from(vec![&b"a"[..], b"b", b"\xff"])),
            None,
        ));
        let target = DataType::List(Arc::new(Field::new("element", DataType::Utf8, true)));

        assert!(matches!(
            to_parquet(&list, &target),
            Err(Error::InvalidUtf8 { row: 1, .. })
        ));
    }

    #[test]
    fn to_parquet_renames_nested_children_to_the_target() {
        let map = read({
            let mut builder = MapBuilder::new(None, StringBuilder::new(), Int32Builder::new());
            builder.keys().append_value("a");
            builder.values().append_value(1);
            builder.append(true).unwrap();
            Arc::new(builder.finish())
        });
        let target = DataType::Map(
            Arc::new(Field::new(
                "key_value",
                DataType::Struct(Fields::from(vec![
                    Field::new("key", DataType::Utf8, false),
                    Field::new("value", DataType::Int64, true),
                ])),
                false,
            )),
            false,
        );

        assert_eq!(map.data_type(), &map_of(DataType::LargeBinary, DataType::Int64));
        assert_eq!(to_parquet(&map, &target).unwrap().data_type(), &target);

        let structure = read(Arc::new(StructArray::from(vec![(
            Arc::new(Field::new("id", DataType::Int64, true)),
            Arc::new(Int64Array::from(vec![1])) as ArrayRef,
        )])));
        let target = DataType::Struct(Fields::from(vec![Field::new("id", DataType::Int64, false)]));

        assert_eq!(to_parquet(&structure, &target).unwrap().data_type(), &target);
    }

    #[test]
    fn to_parquet_refuses_what_has_no_cast() {
        let array: ArrayRef = Arc::new(Int64Array::from(vec![1]));

        assert!(matches!(
            to_parquet(&array, &DataType::Float32),
            Err(Error::Unsupported { parquet, .. }) if parquet == "Float32"
        ));
    }
}
