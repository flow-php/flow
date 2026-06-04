use arrow_array::builder::{
    BinaryBuilder, BooleanBuilder, Date32Builder, Decimal128Builder, FixedSizeBinaryBuilder,
    Float32Builder, Float64Builder, Int16Builder, Int32Builder, Int64Builder, Int8Builder,
    StringBuilder, Time64MicrosecondBuilder, TimestampMicrosecondBuilder, UInt16Builder,
    UInt32Builder, UInt64Builder, UInt8Builder,
};
use arrow_array::*;
use arrow_buffer::{NullBuffer, OffsetBuffer, ScalarBuffer};
use arrow_schema::{
    extension::{Json, Uuid},
    DataType, Field, Fields, Schema, TimeUnit,
};
use ext_php_rs::convert::IntoZvalDyn;
use ext_php_rs::prelude::*;
use ext_php_rs::types::{ArrayKey, ZendHashTable, ZendObject, Zval};
use ext_php_rs::zend::ClassEntry;
use std::sync::Arc;

use crate::parquet::exception::parquet_exception;

fn create_datetime_immutable(seconds: i64, microseconds: i64) -> PhpResult<Zval> {
    let mut callable_ht = ZendHashTable::with_capacity(2);
    let mut class_zv = Zval::new();
    class_zv
        .set_string("DateTimeImmutable", false)
        .map_err(|_| parquet_exception("Failed to create DateTimeImmutable callable"))?;
    callable_ht
        .push(class_zv)
        .map_err(|_| parquet_exception("Failed to create DateTimeImmutable callable"))?;
    let mut method_zv = Zval::new();
    method_zv
        .set_string("createFromFormat", false)
        .map_err(|_| parquet_exception("Failed to create DateTimeImmutable callable"))?;
    callable_ht
        .push(method_zv)
        .map_err(|_| parquet_exception("Failed to create DateTimeImmutable callable"))?;

    let mut callable = Zval::new();
    callable.set_hashtable(callable_ht);

    let format = "U.u".to_string();
    let value = format!("{}.{:06}", seconds, microseconds.unsigned_abs());

    callable
        .try_call(vec![
            &format as &dyn IntoZvalDyn,
            &value as &dyn IntoZvalDyn,
        ])
        .map_err(|e| {
            parquet_exception(format!(
                "Failed to call DateTimeImmutable::createFromFormat: {:?}",
                e
            ))
        })
}

fn create_date_interval(total_microseconds: i64) -> PhpResult<Zval> {
    let hours = total_microseconds / 3_600_000_000;
    let minutes = (total_microseconds / 60_000_000) % 60;
    let secs = (total_microseconds / 1_000_000) % 60;
    let remaining_us = total_microseconds % 1_000_000;

    let spec = format!("PT{}H{}M{}S", hours, minutes, secs);

    let ce = ClassEntry::try_find("DateInterval")
        .ok_or_else(|| parquet_exception("DateInterval class not found"))?;
    let mut obj = ZendObject::new(ce);

    obj.try_call_method("__construct", vec![&spec as &dyn IntoZvalDyn])
        .map_err(|e| parquet_exception(format!("Failed to construct DateInterval: {:?}", e)))?;

    if remaining_us > 0 {
        let fraction = remaining_us as f64 / 1_000_000.0;
        obj.set_property("f", fraction).map_err(|e| {
            parquet_exception(format!("Failed to set DateInterval microseconds: {:?}", e))
        })?;
    }

    let mut zv = Zval::new();
    zv.set_object(&mut obj);
    Ok(zv)
}

fn extract_datetime_timestamp(zv: &Zval, column_name: &str) -> Result<(i64, i64), String> {
    let obj = zv
        .object()
        .ok_or_else(|| format!("Column '{}': expected object", column_name))?;

    let dt_ce = ClassEntry::try_find("DateTimeInterface")
        .ok_or_else(|| format!("Column '{}': DateTimeInterface not found", column_name))?;

    if !obj.instance_of(dt_ce) {
        return Err(format!(
            "Column '{}': expected DateTimeInterface or int, got object",
            column_name
        ));
    }

    let ts_zv = obj
        .try_call_method("getTimestamp", vec![])
        .map_err(|e| format!("Column '{}': getTimestamp() failed: {:?}", column_name, e))?;
    let seconds = ts_zv.long().ok_or_else(|| {
        format!(
            "Column '{}': getTimestamp() did not return int",
            column_name
        )
    })?;

    let format_u = "u".to_string();
    let us_zv = obj
        .try_call_method("format", vec![&format_u as &dyn IntoZvalDyn])
        .map_err(|e| format!("Column '{}': format('u') failed: {:?}", column_name, e))?;
    let us_str = us_zv.str().ok_or_else(|| {
        format!(
            "Column '{}': format('u') did not return string",
            column_name
        )
    })?;
    let microseconds: i64 = us_str.parse().unwrap_or(0);

    Ok((seconds, microseconds))
}

fn extract_date_interval_microseconds(zv: &Zval, column_name: &str) -> Result<i64, String> {
    let obj = zv
        .object()
        .ok_or_else(|| format!("Column '{}': expected object", column_name))?;

    let di_ce = ClassEntry::try_find("DateInterval")
        .ok_or_else(|| format!("Column '{}': DateInterval not found", column_name))?;

    if !obj.instance_of(di_ce) {
        return Err(format!(
            "Column '{}': expected DateInterval or int, got object",
            column_name
        ));
    }

    let h: i64 = obj.get_property::<i64>("h").unwrap_or(0);
    let i: i64 = obj.get_property::<i64>("i").unwrap_or(0);
    let s: i64 = obj.get_property::<i64>("s").unwrap_or(0);
    let f: f64 = obj.get_property::<f64>("f").unwrap_or(0.0);

    Ok(h * 3_600_000_000 + i * 60_000_000 + s * 1_000_000 + (f * 1_000_000.0) as i64)
}

fn downcast_array<'a, T: 'static>(array: &'a dyn Array, type_name: &str) -> PhpResult<&'a T> {
    array
        .as_any()
        .downcast_ref::<T>()
        .ok_or_else(|| parquet_exception(format!("Internal error: expected {}", type_name)))
}

fn convert_primitive_values<T: ArrowPrimitiveType>(
    arr: &PrimitiveArray<T>,
    convert: impl Fn(T::Native) -> PhpResult<Zval>,
) -> PhpResult<Vec<Zval>> {
    let len = arr.len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        if arr.is_null(i) {
            let mut zv = Zval::new();
            zv.set_null();
            result.push(zv);
        } else {
            result.push(convert(arr.value(i))?);
        }
    }
    Ok(result)
}

fn convert_string_values<O: OffsetSizeTrait>(arr: &GenericStringArray<O>) -> PhpResult<Vec<Zval>> {
    let len = arr.len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let mut zv = Zval::new();
        if arr.is_null(i) {
            zv.set_null();
        } else {
            zv.set_string(arr.value(i), false)
                .map_err(|_| parquet_exception("Failed to set string value"))?;
        }
        result.push(zv);
    }
    Ok(result)
}

fn convert_binary_values<O: OffsetSizeTrait>(arr: &GenericBinaryArray<O>) -> Vec<Zval> {
    let len = arr.len();
    let mut result = Vec::with_capacity(len);
    for i in 0..len {
        let mut zv = Zval::new();
        if arr.is_null(i) {
            zv.set_null();
        } else {
            zv.set_binary(arr.value(i).to_vec());
        }
        result.push(zv);
    }
    result
}

fn convert_single_arrow_value(array: &dyn Array, index: usize) -> PhpResult<Zval> {
    if array.is_null(index) {
        let mut zv = Zval::new();
        zv.set_null();
        return Ok(zv);
    }

    match array.data_type() {
        DataType::Boolean => {
            let arr = downcast_array::<BooleanArray>(array, "BooleanArray")?;
            let mut zv = Zval::new();
            zv.set_bool(arr.value(index));
            Ok(zv)
        }
        DataType::Int8 => {
            let arr = downcast_array::<Int8Array>(array, "Int8Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::Int16 => {
            let arr = downcast_array::<Int16Array>(array, "Int16Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::Int32 => {
            let arr = downcast_array::<Int32Array>(array, "Int32Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::Int64 => {
            let arr = downcast_array::<Int64Array>(array, "Int64Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index));
            Ok(zv)
        }
        DataType::UInt8 => {
            let arr = downcast_array::<UInt8Array>(array, "UInt8Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::UInt16 => {
            let arr = downcast_array::<UInt16Array>(array, "UInt16Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::UInt32 => {
            let arr = downcast_array::<UInt32Array>(array, "UInt32Array")?;
            let mut zv = Zval::new();
            zv.set_long(arr.value(index) as i64);
            Ok(zv)
        }
        DataType::UInt64 => {
            let arr = downcast_array::<UInt64Array>(array, "UInt64Array")?;
            let val = arr.value(index);
            let mut zv = Zval::new();
            if val > i64::MAX as u64 {
                zv.set_string(&val.to_string(), false)
                    .map_err(|_| parquet_exception("Failed to set uint64 string value"))?;
            } else {
                zv.set_long(val as i64);
            }
            Ok(zv)
        }
        DataType::Float32 => {
            let arr = downcast_array::<Float32Array>(array, "Float32Array")?;
            let mut zv = Zval::new();
            zv.set_double(arr.value(index) as f64);
            Ok(zv)
        }
        DataType::Float64 => {
            let arr = downcast_array::<Float64Array>(array, "Float64Array")?;
            let mut zv = Zval::new();
            zv.set_double(arr.value(index));
            Ok(zv)
        }
        DataType::Utf8 => {
            let arr = downcast_array::<StringArray>(array, "StringArray")?;
            let mut zv = Zval::new();
            zv.set_string(arr.value(index), false)
                .map_err(|_| parquet_exception("Failed to set string value"))?;
            Ok(zv)
        }
        DataType::LargeUtf8 => {
            let arr = downcast_array::<LargeStringArray>(array, "LargeStringArray")?;
            let mut zv = Zval::new();
            zv.set_string(arr.value(index), false)
                .map_err(|_| parquet_exception("Failed to set string value"))?;
            Ok(zv)
        }
        DataType::Binary => {
            let arr = downcast_array::<BinaryArray>(array, "BinaryArray")?;
            let mut zv = Zval::new();
            zv.set_binary(arr.value(index).to_vec());
            Ok(zv)
        }
        DataType::LargeBinary => {
            let arr = downcast_array::<LargeBinaryArray>(array, "LargeBinaryArray")?;
            let mut zv = Zval::new();
            zv.set_binary(arr.value(index).to_vec());
            Ok(zv)
        }
        DataType::FixedSizeBinary(_) => {
            let arr = downcast_array::<FixedSizeBinaryArray>(array, "FixedSizeBinaryArray")?;
            let mut zv = Zval::new();
            zv.set_binary(arr.value(index).to_vec());
            Ok(zv)
        }
        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(array, "Date32Array")?;
            create_datetime_immutable(arr.value(index) as i64 * 86400, 0)
        }
        DataType::Date64 => {
            let arr = downcast_array::<Date64Array>(array, "Date64Array")?;
            let ms = arr.value(index);
            create_datetime_immutable(ms / 1000, (ms % 1000) * 1000)
        }
        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = downcast_array::<TimestampSecondArray>(array, "TimestampSecondArray")?;
            create_datetime_immutable(arr.value(index), 0)
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr =
                downcast_array::<TimestampMillisecondArray>(array, "TimestampMillisecondArray")?;
            let v = arr.value(index);
            create_datetime_immutable(v / 1000, (v % 1000) * 1000)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr =
                downcast_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
            let v = arr.value(index);
            create_datetime_immutable(v / 1_000_000, v % 1_000_000)
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr =
                downcast_array::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?;
            let v = arr.value(index);
            create_datetime_immutable(v / 1_000_000_000, (v / 1000) % 1_000_000)
        }
        DataType::Time32(TimeUnit::Second) => {
            let arr = downcast_array::<Time32SecondArray>(array, "Time32SecondArray")?;
            create_date_interval(arr.value(index) as i64 * 1_000_000)
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = downcast_array::<Time32MillisecondArray>(array, "Time32MillisecondArray")?;
            create_date_interval(arr.value(index) as i64 * 1000)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = downcast_array::<Time64MicrosecondArray>(array, "Time64MicrosecondArray")?;
            create_date_interval(arr.value(index))
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = downcast_array::<Time64NanosecondArray>(array, "Time64NanosecondArray")?;
            create_date_interval(arr.value(index) / 1000)
        }
        DataType::Decimal128(_, _) => {
            let arr = downcast_array::<Decimal128Array>(array, "Decimal128Array")?;
            let str_val = arr.value_as_string(index);
            let float_val: f64 = str_val.parse().map_err(|e| {
                parquet_exception(format!(
                    "Failed to parse decimal value '{}': {}",
                    str_val, e
                ))
            })?;
            let mut zv = Zval::new();
            zv.set_double(float_val);
            Ok(zv)
        }
        DataType::List(_) => {
            let list_array = downcast_array::<ListArray>(array, "ListArray")?;
            let values = list_array.value(index);
            let mut ht = ZendHashTable::with_capacity(values.len() as u32);
            for j in 0..values.len() {
                let child_zv = convert_single_arrow_value(values.as_ref(), j)?;
                ht.push(child_zv)
                    .map_err(|_| parquet_exception("Failed to build list element"))?;
            }
            let mut zv = Zval::new();
            zv.set_hashtable(ht);
            Ok(zv)
        }
        DataType::LargeList(_) => {
            let list_array = downcast_array::<LargeListArray>(array, "LargeListArray")?;
            let values = list_array.value(index);
            let mut ht = ZendHashTable::with_capacity(values.len() as u32);
            for j in 0..values.len() {
                let child_zv = convert_single_arrow_value(values.as_ref(), j)?;
                ht.push(child_zv)
                    .map_err(|_| parquet_exception("Failed to build list element"))?;
            }
            let mut zv = Zval::new();
            zv.set_hashtable(ht);
            Ok(zv)
        }
        DataType::Struct(fields) => {
            let struct_array = downcast_array::<StructArray>(array, "StructArray")?;
            let mut ht = ZendHashTable::with_capacity(fields.len() as u32);
            for (col_idx, field) in fields.iter().enumerate() {
                let child_array = struct_array.column(col_idx);
                let child_zv = convert_single_arrow_value(child_array.as_ref(), index)?;
                ht.insert(field.name().as_str(), child_zv)
                    .map_err(|_| parquet_exception("Failed to build struct field"))?;
            }
            let mut zv = Zval::new();
            zv.set_hashtable(ht);
            Ok(zv)
        }
        DataType::Map(_, _) => {
            let map_array = downcast_array::<MapArray>(array, "MapArray")?;
            let entries = map_array.value(index);
            let keys_array = entries.column(0);
            let values_array = entries.column(1);
            let mut ht = ZendHashTable::with_capacity(entries.len() as u32);
            for j in 0..entries.len() {
                let key_zv = convert_single_arrow_value(keys_array.as_ref(), j)?;
                let val_zv = convert_single_arrow_value(values_array.as_ref(), j)?;
                let key_str = if let Some(s) = key_zv.str() {
                    s.to_string()
                } else if let Some(l) = key_zv.long() {
                    l.to_string()
                } else {
                    j.to_string()
                };
                ht.insert(key_str.as_str(), val_zv)
                    .map_err(|_| parquet_exception("Failed to build map entry"))?;
            }
            let mut zv = Zval::new();
            zv.set_hashtable(ht);
            Ok(zv)
        }
        other => Err(parquet_exception(format!(
            "Unsupported Arrow type in nested conversion: {:?}",
            other
        ))),
    }
}

pub fn arrow_array_to_php_values(array: &dyn Array, field: Option<&Field>) -> PhpResult<Vec<Zval>> {
    match array.data_type() {
        DataType::Boolean => {
            let arr = downcast_array::<BooleanArray>(array, "BooleanArray")?;
            let len = arr.len();
            let mut result = Vec::with_capacity(len);
            for i in 0..len {
                let mut zv = Zval::new();
                if arr.is_null(i) {
                    zv.set_null();
                } else {
                    zv.set_bool(arr.value(i));
                }
                result.push(zv);
            }
            Ok(result)
        }

        DataType::Int8 => {
            let arr = downcast_array::<Int8Array>(array, "Int8Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }
        DataType::Int16 => {
            let arr = downcast_array::<Int16Array>(array, "Int16Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }
        DataType::Int32 => {
            let arr = downcast_array::<Int32Array>(array, "Int32Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }
        DataType::Int64 => {
            let arr = downcast_array::<Int64Array>(array, "Int64Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v);
                Ok(zv)
            })
        }
        DataType::UInt8 => {
            let arr = downcast_array::<UInt8Array>(array, "UInt8Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }
        DataType::UInt16 => {
            let arr = downcast_array::<UInt16Array>(array, "UInt16Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }
        DataType::UInt32 => {
            let arr = downcast_array::<UInt32Array>(array, "UInt32Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_long(v as i64);
                Ok(zv)
            })
        }

        DataType::UInt64 => {
            let arr = downcast_array::<UInt64Array>(array, "UInt64Array")?;
            convert_primitive_values(arr, |val| {
                let mut zv = Zval::new();
                if val > i64::MAX as u64 {
                    zv.set_string(&val.to_string(), false)
                        .map_err(|_| parquet_exception("Failed to set uint64 string value"))?;
                } else {
                    zv.set_long(val as i64);
                }
                Ok(zv)
            })
        }

        DataType::Float32 => {
            let arr = downcast_array::<Float32Array>(array, "Float32Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_double(v as f64);
                Ok(zv)
            })
        }
        DataType::Float64 => {
            let arr = downcast_array::<Float64Array>(array, "Float64Array")?;
            convert_primitive_values(arr, |v| {
                let mut zv = Zval::new();
                zv.set_double(v);
                Ok(zv)
            })
        }

        DataType::Utf8 => {
            let arr = downcast_array::<StringArray>(array, "StringArray")?;
            convert_string_values(arr)
        }
        DataType::LargeUtf8 => {
            let arr = downcast_array::<LargeStringArray>(array, "LargeStringArray")?;
            convert_string_values(arr)
        }

        DataType::Binary => {
            let arr = downcast_array::<BinaryArray>(array, "BinaryArray")?;
            Ok(convert_binary_values(arr))
        }
        DataType::LargeBinary => {
            let arr = downcast_array::<LargeBinaryArray>(array, "LargeBinaryArray")?;
            Ok(convert_binary_values(arr))
        }

        DataType::FixedSizeBinary(_) => {
            let arr = downcast_array::<FixedSizeBinaryArray>(array, "FixedSizeBinaryArray")?;
            let is_uuid = field.is_some_and(|f| f.try_extension_type::<Uuid>().is_ok());
            let len = arr.len();
            let mut result = Vec::with_capacity(len);
            for i in 0..len {
                let mut zv = Zval::new();
                if arr.is_null(i) {
                    zv.set_null();
                } else if is_uuid && arr.value(i).len() == 16 {
                    let b = arr.value(i);
                    let uuid = format!(
                        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
                        b[0], b[1], b[2], b[3], b[4], b[5], b[6], b[7],
                        b[8], b[9], b[10], b[11], b[12], b[13], b[14], b[15]
                    );
                    zv.set_string(&uuid, false)
                        .map_err(|_| parquet_exception("Failed to set UUID string value"))?;
                } else {
                    zv.set_binary(arr.value(i).to_vec());
                }
                result.push(zv);
            }
            Ok(result)
        }

        DataType::Date32 => {
            let arr = downcast_array::<Date32Array>(array, "Date32Array")?;
            convert_primitive_values(arr, |v| create_datetime_immutable(v as i64 * 86400, 0))
        }
        DataType::Date64 => {
            let arr = downcast_array::<Date64Array>(array, "Date64Array")?;
            convert_primitive_values(arr, |v| {
                create_datetime_immutable(v / 1000, (v % 1000) * 1000)
            })
        }

        DataType::Timestamp(TimeUnit::Second, _) => {
            let arr = downcast_array::<TimestampSecondArray>(array, "TimestampSecondArray")?;
            convert_primitive_values(arr, |v| create_datetime_immutable(v, 0))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => {
            let arr =
                downcast_array::<TimestampMillisecondArray>(array, "TimestampMillisecondArray")?;
            convert_primitive_values(arr, |v| {
                create_datetime_immutable(v / 1000, (v % 1000) * 1000)
            })
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let arr =
                downcast_array::<TimestampMicrosecondArray>(array, "TimestampMicrosecondArray")?;
            convert_primitive_values(arr, |v| {
                create_datetime_immutable(v / 1_000_000, v % 1_000_000)
            })
        }
        DataType::Timestamp(TimeUnit::Nanosecond, _) => {
            let arr =
                downcast_array::<TimestampNanosecondArray>(array, "TimestampNanosecondArray")?;
            convert_primitive_values(arr, |v| {
                create_datetime_immutable(v / 1_000_000_000, (v / 1000) % 1_000_000)
            })
        }

        DataType::Time32(TimeUnit::Second) => {
            let arr = downcast_array::<Time32SecondArray>(array, "Time32SecondArray")?;
            convert_primitive_values(arr, |v| create_date_interval(v as i64 * 1_000_000))
        }
        DataType::Time32(TimeUnit::Millisecond) => {
            let arr = downcast_array::<Time32MillisecondArray>(array, "Time32MillisecondArray")?;
            convert_primitive_values(arr, |v| create_date_interval(v as i64 * 1000))
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let arr = downcast_array::<Time64MicrosecondArray>(array, "Time64MicrosecondArray")?;
            convert_primitive_values(arr, create_date_interval)
        }
        DataType::Time64(TimeUnit::Nanosecond) => {
            let arr = downcast_array::<Time64NanosecondArray>(array, "Time64NanosecondArray")?;
            convert_primitive_values(arr, |v| create_date_interval(v / 1000))
        }

        DataType::Decimal128(_, _) => {
            let arr = downcast_array::<Decimal128Array>(array, "Decimal128Array")?;
            let len = arr.len();
            let mut result = Vec::with_capacity(len);
            for i in 0..len {
                let mut zv = Zval::new();
                if arr.is_null(i) {
                    zv.set_null();
                } else {
                    let str_val = arr.value_as_string(i);
                    let float_val: f64 = str_val.parse().map_err(|e| {
                        parquet_exception(format!(
                            "Failed to parse decimal value '{}': {}",
                            str_val, e
                        ))
                    })?;
                    zv.set_double(float_val);
                }
                result.push(zv);
            }
            Ok(result)
        }

        DataType::List(_) | DataType::LargeList(_) | DataType::Struct(_) | DataType::Map(_, _) => {
            let len = array.len();
            let mut result = Vec::with_capacity(len);
            for i in 0..len {
                result.push(convert_single_arrow_value(array, i)?);
            }
            Ok(result)
        }

        other => Err(parquet_exception(format!(
            "Unsupported Arrow array type: {:?}",
            other
        ))),
    }
}

fn zval_type_name(zv: &Zval) -> &'static str {
    if zv.is_null() {
        "null"
    } else if zv.is_bool() {
        "bool"
    } else if zv.is_long() {
        "int"
    } else if zv.is_double() {
        "float"
    } else if zv.is_string() {
        "string"
    } else if zv.is_array() {
        "array"
    } else if zv.is_object() {
        "object"
    } else {
        "unknown"
    }
}

fn parse_decimal_string(s: &str, scale: i8) -> Result<i128, String> {
    let trimmed = s.trim();
    if trimmed.is_empty() {
        return Err("Empty decimal string".into());
    }

    let (negative, digits_str) = if let Some(stripped) = trimmed.strip_prefix('-') {
        (true, stripped)
    } else if let Some(stripped) = trimmed.strip_prefix('+') {
        (false, stripped)
    } else {
        (false, trimmed)
    };

    let (integer_part, fractional_part) = if let Some(dot_pos) = digits_str.find('.') {
        (&digits_str[..dot_pos], &digits_str[dot_pos + 1..])
    } else {
        (digits_str, "")
    };

    if !integer_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!("Invalid decimal integer part: '{}'", integer_part));
    }
    if !fractional_part.chars().all(|c| c.is_ascii_digit()) {
        return Err(format!(
            "Invalid decimal fractional part: '{}'",
            fractional_part
        ));
    }

    let scale_usize = scale as usize;
    let adjusted_frac = if fractional_part.len() >= scale_usize {
        fractional_part[..scale_usize].to_string()
    } else {
        format!("{:0<width$}", fractional_part, width = scale_usize)
    };

    let combined = format!("{}{}", integer_part, adjusted_frac);
    let value: i128 = combined
        .parse()
        .map_err(|e| format!("Failed to parse decimal '{}': {}", s, e))?;

    Ok(if negative { -value } else { value })
}

macro_rules! build_int_array {
    ($values:expr, $column_name:expr, $builder_type:ty, $cast_type:ty) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for zv in $values {
            if zv.is_null() {
                builder.append_null();
            } else if let Some(v) = zv.long() {
                let converted = <$cast_type>::try_from(v).map_err(|_| {
                    format!(
                        "Column '{}': value {} out of range for {} (valid range: {} to {})",
                        $column_name,
                        v,
                        stringify!($cast_type),
                        <$cast_type>::MIN,
                        <$cast_type>::MAX
                    )
                })?;
                builder.append_value(converted);
            } else {
                return Err(format!(
                    "Column '{}': expected int, got {}",
                    $column_name,
                    zval_type_name(zv)
                ));
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

macro_rules! build_float_array {
    ($values:expr, $column_name:expr, $builder_type:ty, $cast_type:ty) => {{
        let mut builder = <$builder_type>::with_capacity($values.len());
        for zv in $values {
            if zv.is_null() {
                builder.append_null();
            } else if let Some(v) = zv.double() {
                let converted = v as $cast_type;
                if converted.is_infinite() && !v.is_infinite() {
                    return Err(format!(
                        "Column '{}': value {} overflows {}",
                        $column_name,
                        v,
                        stringify!($cast_type)
                    ));
                }
                builder.append_value(converted);
            } else if let Some(v) = zv.long() {
                builder.append_value(v as $cast_type);
            } else {
                return Err(format!(
                    "Column '{}': expected float or int, got {}",
                    $column_name,
                    zval_type_name(zv)
                ));
            }
        }
        Ok(Arc::new(builder.finish()) as ArrayRef)
    }};
}

fn php_schema_entry_to_field(entry_ht: &ZendHashTable) -> Result<Field, String> {
    let name = entry_ht
        .get("name")
        .and_then(|z| z.str())
        .ok_or_else(|| "Schema entry must have a 'name' string key".to_string())?
        .to_string();

    let type_str = entry_ht
        .get("type")
        .and_then(|z| z.str())
        .ok_or_else(|| "Schema entry must have a 'type' string key".to_string())?
        .to_string();

    let nullable = entry_ht
        .get("optional")
        .and_then(|z| z.bool())
        .unwrap_or(true);

    let data_type = match type_str.as_str() {
        "BOOLEAN" => DataType::Boolean,
        "INT8" => DataType::Int8,
        "INT16" => DataType::Int16,
        "INT32" => DataType::Int32,
        "INT64" => DataType::Int64,
        "UINT8" => DataType::UInt8,
        "UINT16" => DataType::UInt16,
        "UINT32" => DataType::UInt32,
        "UINT64" => DataType::UInt64,
        "FLOAT" => DataType::Float32,
        "DOUBLE" => DataType::Float64,
        "STRING" => DataType::Utf8,
        "BINARY" => DataType::Binary,
        "DATE" => DataType::Date32,
        "TIMESTAMP" => DataType::Timestamp(TimeUnit::Microsecond, None),
        "TIME" => DataType::Time64(TimeUnit::Microsecond),
        "DECIMAL" => {
            let precision = entry_ht
                .get("precision")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': DECIMAL type requires 'precision' integer key",
                        name
                    )
                })? as u8;
            let scale = entry_ht
                .get("scale")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': DECIMAL type requires 'scale' integer key",
                        name
                    )
                })? as i8;
            DataType::Decimal128(precision, scale)
        }
        "UUID" => {
            let mut field = Field::new(&name, DataType::FixedSizeBinary(16), nullable);
            field.try_with_extension_type(Uuid).map_err(|e| {
                format!(
                    "Column '{}': failed to set UUID extension type: {}",
                    name, e
                )
            })?;
            return Ok(field);
        }
        "JSON" => {
            let mut field = Field::new(&name, DataType::Utf8, nullable);
            field
                .try_with_extension_type(Json::default())
                .map_err(|e| {
                    format!(
                        "Column '{}': failed to set JSON extension type: {}",
                        name, e
                    )
                })?;
            return Ok(field);
        }
        "FIXED_SIZE_BINARY" => {
            let length = entry_ht
                .get("length")
                .and_then(|z| z.long())
                .ok_or_else(|| {
                    format!(
                        "Column '{}': FIXED_SIZE_BINARY type requires 'length' integer key",
                        name
                    )
                })? as i32;
            DataType::FixedSizeBinary(length)
        }
        "LIST" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| format!("Column '{}': LIST type requires 'children' array", name))?;
            let child_zv = children.values().next().ok_or_else(|| {
                format!("Column '{}': LIST type requires exactly one child", name)
            })?;
            let child_ht = child_zv
                .array()
                .ok_or_else(|| format!("Column '{}': LIST child must be an array", name))?;
            let child_field = php_schema_entry_to_field(child_ht)?;
            DataType::List(Arc::new(child_field))
        }
        "STRUCT" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| {
                    format!("Column '{}': STRUCT type requires 'children' array", name)
                })?;
            let fields: Result<Vec<Field>, String> = children
                .values()
                .map(|child_zv| {
                    let child_ht = child_zv.array().ok_or_else(|| {
                        format!("Column '{}': STRUCT child must be an array", name)
                    })?;
                    php_schema_entry_to_field(child_ht)
                })
                .collect();
            DataType::Struct(Fields::from(fields?))
        }
        "MAP" => {
            let children = entry_ht
                .get("children")
                .and_then(|z| z.array())
                .ok_or_else(|| format!("Column '{}': MAP type requires 'children' array", name))?;
            let mut child_iter = children.values();
            let key_zv = child_iter.next().ok_or_else(|| {
                format!(
                    "Column '{}': MAP type requires key and value children",
                    name
                )
            })?;
            let val_zv = child_iter.next().ok_or_else(|| {
                format!(
                    "Column '{}': MAP type requires key and value children",
                    name
                )
            })?;
            let key_field =
                php_schema_entry_to_field(key_zv.array().ok_or_else(|| {
                    format!("Column '{}': MAP key child must be an array", name)
                })?)?;
            let val_field =
                php_schema_entry_to_field(val_zv.array().ok_or_else(|| {
                    format!("Column '{}': MAP value child must be an array", name)
                })?)?;
            let entries_struct = DataType::Struct(Fields::from(vec![key_field, val_field]));
            DataType::Map(
                Arc::new(Field::new("entries", entries_struct, false)),
                false,
            )
        }
        other => {
            return Err(format!("Column '{}': Unsupported type '{}'", name, other));
        }
    };

    Ok(Field::new(&name, data_type, nullable))
}

pub fn php_schema_to_arrow(schema: &ZendHashTable) -> Result<Schema, String> {
    if schema.is_empty() {
        return Err("Schema must have at least one column".into());
    }

    let mut fields = Vec::with_capacity(schema.len());

    for (_key, zv) in schema.iter() {
        let entry_ht = zv
            .array()
            .ok_or_else(|| "Schema entry must be an array".to_string())?;
        fields.push(php_schema_entry_to_field(entry_ht)?);
    }

    Ok(Schema::new(fields))
}

pub fn php_array_to_arrow(
    values: &[&Zval],
    data_type: &DataType,
    column_name: &str,
) -> Result<ArrayRef, String> {
    match data_type {
        DataType::Boolean => {
            let mut builder = BooleanBuilder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(v) = zv.bool() {
                    builder.append_value(v);
                } else {
                    return Err(format!(
                        "Column '{}': expected bool, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Int8 => build_int_array!(values, column_name, Int8Builder, i8),
        DataType::Int16 => build_int_array!(values, column_name, Int16Builder, i16),
        DataType::Int32 => build_int_array!(values, column_name, Int32Builder, i32),
        DataType::Int64 => build_int_array!(values, column_name, Int64Builder, i64),
        DataType::UInt8 => build_int_array!(values, column_name, UInt8Builder, u8),
        DataType::UInt16 => build_int_array!(values, column_name, UInt16Builder, u16),
        DataType::UInt32 => build_int_array!(values, column_name, UInt32Builder, u32),
        DataType::UInt64 => {
            let mut builder = UInt64Builder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(v) = zv.long() {
                    let converted =
                        u64::try_from(v).map_err(|_| {
                            format!(
                            "Column '{}': value {} out of range for u64 (valid range: {} to {})",
                            column_name, v, u64::MIN, u64::MAX
                        )
                        })?;
                    builder.append_value(converted);
                } else if let Some(s) = zv.str() {
                    let parsed: u64 = s.parse().map_err(|e| {
                        format!(
                            "Column '{}': failed to parse '{}' as u64: {}",
                            column_name, s, e
                        )
                    })?;
                    builder.append_value(parsed);
                } else {
                    return Err(format!(
                        "Column '{}': expected int or string, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Float32 => build_float_array!(values, column_name, Float32Builder, f32),
        DataType::Float64 => build_float_array!(values, column_name, Float64Builder, f64),
        DataType::Utf8 => {
            let mut builder = StringBuilder::with_capacity(values.len(), values.len() * 32);
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(s) = zv.str() {
                    builder.append_value(s);
                } else {
                    return Err(format!(
                        "Column '{}': expected string, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Binary => {
            let mut builder = BinaryBuilder::with_capacity(values.len(), values.len() * 32);
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(zend_str) = zv.zend_str() {
                    builder.append_value(zend_str.as_bytes());
                } else {
                    return Err(format!(
                        "Column '{}': expected string (binary), got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::FixedSizeBinary(n) => {
            let mut builder = FixedSizeBinaryBuilder::with_capacity(values.len(), *n);
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(zend_str) = zv.zend_str() {
                    let bytes = zend_str.as_bytes();
                    if bytes.len() == *n as usize {
                        builder
                            .append_value(bytes)
                            .map_err(|e| format!("Column '{}': {}", column_name, e))?;
                    } else if *n == 16 && (bytes.len() == 36 || bytes.len() == 32) {
                        let hex: String = bytes
                            .iter()
                            .filter(|b| **b != b'-')
                            .map(|b| *b as char)
                            .collect();
                        if hex.len() != 32 {
                            return Err(format!("Column '{}': invalid UUID string", column_name));
                        }
                        let mut uuid_bytes = [0u8; 16];
                        for i in 0..16 {
                            uuid_bytes[i] = u8::from_str_radix(&hex[i * 2..i * 2 + 2], 16)
                                .map_err(|e| {
                                    format!("Column '{}': invalid UUID hex: {}", column_name, e)
                                })?;
                        }
                        builder
                            .append_value(uuid_bytes)
                            .map_err(|e| format!("Column '{}': {}", column_name, e))?;
                    } else {
                        return Err(format!(
                            "Column '{}': expected {} bytes for FIXED_SIZE_BINARY, got {}",
                            column_name,
                            n,
                            bytes.len()
                        ));
                    }
                } else {
                    return Err(format!(
                        "Column '{}': expected string (binary), got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Date32 => {
            let mut builder = Date32Builder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(v) = zv.long() {
                    builder.append_value((v / 86400) as i32);
                } else if zv.is_object() {
                    let (seconds, _) = extract_datetime_timestamp(zv, column_name)?;
                    builder.append_value((seconds / 86400) as i32);
                } else {
                    return Err(format!(
                        "Column '{}': expected DateTimeInterface or int, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            let mut builder = TimestampMicrosecondBuilder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(v) = zv.long() {
                    builder.append_value(v);
                } else if zv.is_object() {
                    let (seconds, microseconds) = extract_datetime_timestamp(zv, column_name)?;
                    builder.append_value(seconds * 1_000_000 + microseconds);
                } else {
                    return Err(format!(
                        "Column '{}': expected DateTimeInterface or int, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Time64(TimeUnit::Microsecond) => {
            let mut builder = Time64MicrosecondBuilder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(v) = zv.long() {
                    builder.append_value(v);
                } else if zv.is_object() {
                    builder.append_value(extract_date_interval_microseconds(zv, column_name)?);
                } else {
                    return Err(format!(
                        "Column '{}': expected DateInterval or int, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }
        DataType::Decimal128(p, s) => {
            let mut builder = Decimal128Builder::with_capacity(values.len());
            for zv in values {
                if zv.is_null() {
                    builder.append_null();
                } else if let Some(str_val) = zv.str() {
                    let unscaled = parse_decimal_string(str_val, *s)?;
                    builder.append_value(unscaled);
                } else if let Some(v) = zv.double() {
                    let str_val = format!("{:.prec$}", v, prec = *s as usize);
                    let unscaled = parse_decimal_string(&str_val, *s)?;
                    builder.append_value(unscaled);
                } else if let Some(v) = zv.long() {
                    let str_val = format!("{}.{}", v, "0".repeat(*s as usize));
                    let unscaled = parse_decimal_string(&str_val, *s)?;
                    builder.append_value(unscaled);
                } else {
                    return Err(format!(
                        "Column '{}': expected string, float, or int for DECIMAL, got {}",
                        column_name,
                        zval_type_name(zv)
                    ));
                }
            }
            let array = builder
                .finish()
                .with_precision_and_scale(*p, *s)
                .map_err(|e| format!("Column '{}': {}", column_name, e))?;
            Ok(Arc::new(array) as ArrayRef)
        }
        DataType::List(child_field) => {
            let mut offsets = vec![0i32];
            let mut null_bits: Vec<bool> = Vec::with_capacity(values.len());
            let mut child_refs: Vec<&Zval> = Vec::new();

            for zv in values {
                if zv.is_null() {
                    null_bits.push(false);
                    offsets.push(*offsets.last().unwrap());
                } else {
                    null_bits.push(true);
                    let arr = zv.array().ok_or_else(|| {
                        format!(
                            "Column '{}': expected array for LIST, got {}",
                            column_name,
                            zval_type_name(zv)
                        )
                    })?;
                    for (_key, elem) in arr.iter() {
                        child_refs.push(elem);
                    }
                    offsets.push(child_refs.len() as i32);
                }
            }

            let child_array = php_array_to_arrow(
                &child_refs,
                child_field.data_type(),
                &format!("{}.item", column_name),
            )?;

            let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
            let null_buffer = NullBuffer::from(null_bits);

            Ok(Arc::new(
                ListArray::try_new(
                    child_field.clone(),
                    offsets_buffer,
                    child_array,
                    Some(null_buffer),
                )
                .map_err(|e| format!("Column '{}': {}", column_name, e))?,
            ) as ArrayRef)
        }
        DataType::Struct(fields) => {
            let null_zval = Zval::new();

            for (row_idx, zv) in values.iter().enumerate() {
                if !zv.is_null() {
                    let arr = zv.array().ok_or_else(|| {
                        format!(
                            "Column '{}': expected array for STRUCT at row {}, got {}",
                            column_name,
                            row_idx,
                            zval_type_name(zv)
                        )
                    })?;
                    for field in fields.iter() {
                        if !field.is_nullable() {
                            let child_val = arr.get(field.name().as_str());
                            if child_val.is_none() || child_val.unwrap().is_null() {
                                return Err(format!(
                                    "Column '{}': non-nullable field '{}' is missing/null at row {}",
                                    column_name,
                                    field.name(),
                                    row_idx
                                ));
                            }
                        }
                    }
                }
            }

            let null_bits: Vec<bool> = values.iter().map(|zv| !zv.is_null()).collect();
            let null_buffer = NullBuffer::from(null_bits);

            let child_arrays: Result<Vec<ArrayRef>, String> = fields
                .iter()
                .map(|field| {
                    let child_refs: Vec<&Zval> = values
                        .iter()
                        .map(|zv| {
                            if zv.is_null() {
                                &null_zval
                            } else {
                                zv.array()
                                    .and_then(|arr| arr.get(field.name().as_str()))
                                    .unwrap_or(&null_zval)
                            }
                        })
                        .collect();

                    php_array_to_arrow(
                        &child_refs,
                        field.data_type(),
                        &format!("{}.{}", column_name, field.name()),
                    )
                })
                .collect();

            Ok(Arc::new(
                StructArray::try_new(fields.clone(), child_arrays?, Some(null_buffer))
                    .map_err(|e| format!("Column '{}': {}", column_name, e))?,
            ) as ArrayRef)
        }
        DataType::Map(entries_field, _) => {
            let (key_field, val_field) = match entries_field.data_type() {
                DataType::Struct(fields) if fields.len() == 2 => (&fields[0], &fields[1]),
                _ => {
                    return Err(format!(
                        "Column '{}': MAP entries must be Struct with key and value",
                        column_name
                    ))
                }
            };

            let mut offsets = vec![0i32];
            let mut null_bits: Vec<bool> = Vec::with_capacity(values.len());
            let mut key_zvals: Vec<Zval> = Vec::new();
            let mut val_refs: Vec<&Zval> = Vec::new();

            for zv in values {
                if zv.is_null() {
                    null_bits.push(false);
                    offsets.push(*offsets.last().unwrap());
                } else {
                    null_bits.push(true);
                    let arr = zv.array().ok_or_else(|| {
                        format!(
                            "Column '{}': expected array for MAP, got {}",
                            column_name,
                            zval_type_name(zv)
                        )
                    })?;
                    for (key, val) in arr.iter() {
                        let mut key_zv = Zval::new();
                        let set_key = |zv: &mut Zval, s: &str| {
                            zv.set_string(s, false).map_err(|_| {
                                format!("Column '{}': failed to set MAP key string", column_name)
                            })
                        };
                        match key {
                            ArrayKey::Long(n) => key_zv.set_long(n),
                            ArrayKey::String(ref s) => set_key(&mut key_zv, s)?,
                            ArrayKey::Str(s) => set_key(&mut key_zv, s)?,
                            ArrayKey::ZendString(zs) => {
                                let s = zs.as_str().map_err(|_| {
                                    format!(
                                        "Column '{}': failed to read MAP key string",
                                        column_name
                                    )
                                })?;
                                set_key(&mut key_zv, s)?;
                            }
                        }
                        key_zvals.push(key_zv);
                        val_refs.push(val);
                    }
                    offsets.push(val_refs.len() as i32);
                }
            }

            let key_refs: Vec<&Zval> = key_zvals.iter().collect();
            let key_array = php_array_to_arrow(
                &key_refs,
                key_field.data_type(),
                &format!("{}.key", column_name),
            )?;
            let val_array = php_array_to_arrow(
                &val_refs,
                val_field.data_type(),
                &format!("{}.value", column_name),
            )?;

            let entries_struct = StructArray::try_new(
                Fields::from(vec![
                    Field::new(key_field.name(), key_field.data_type().clone(), false),
                    Field::new(
                        val_field.name(),
                        val_field.data_type().clone(),
                        val_field.is_nullable(),
                    ),
                ]),
                vec![key_array, val_array],
                None,
            )
            .map_err(|e| format!("Column '{}': {}", column_name, e))?;

            let offsets_buffer = OffsetBuffer::new(ScalarBuffer::from(offsets));
            let null_buffer = NullBuffer::from(null_bits);

            Ok(Arc::new(MapArray::new(
                entries_field.clone(),
                offsets_buffer,
                entries_struct,
                Some(null_buffer),
                false,
            )) as ArrayRef)
        }
        _ => Err(format!(
            "Column '{}': unsupported data type {:?} in writer",
            column_name, data_type
        )),
    }
}
