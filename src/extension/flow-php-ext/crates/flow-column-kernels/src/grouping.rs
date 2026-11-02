//! Group keys equal exactly when PHP's `KeyEncoder` keys are equal: each value carries the tag of its physical.

use arrow_array::cast::AsArray;
use arrow_array::types::{Date32Type, DurationMicrosecondType, Float64Type, Int64Type, TimestampMicrosecondType};
use arrow_array::Array;
use arrow_schema::{DataType, TimeUnit};

const DAY_MICROSECONDS: i64 = 86_400_000_000;

pub fn append_native_key(key: &mut Vec<u8>, array: &dyn Array, row: usize) -> bool {
    match array.data_type() {
        DataType::Null => key.extend_from_slice(b"\0n"),
        _ if array.is_null(row) => key.extend_from_slice(b"\0n"),
        DataType::Boolean => key.extend_from_slice(if array.as_boolean().value(row) { b"\0t" } else { b"\0f" }),
        DataType::Int64 => append_int(key, array.as_primitive::<Int64Type>().value(row)),
        DataType::Timestamp(TimeUnit::Microsecond, _) => {
            append_int(key, array.as_primitive::<TimestampMicrosecondType>().value(row));
        }
        DataType::Duration(TimeUnit::Microsecond) => {
            append_int(key, array.as_primitive::<DurationMicrosecondType>().value(row));
        }
        DataType::Date32 => {
            let days = i64::from(array.as_primitive::<Date32Type>().value(row));

            // a day count past i64 microseconds is a float instant in PHP too
            match days.checked_mul(DAY_MICROSECONDS) {
                Some(instant) => append_int(key, instant),
                None => append_float(key, days as f64 * DAY_MICROSECONDS as f64),
            }
        }
        DataType::Float64 => append_float(key, array.as_primitive::<Float64Type>().value(row)),
        DataType::LargeBinary => append_bytes(key, array.as_binary::<i64>().value(row)),
        DataType::FixedSizeBinary(_) => append_bytes(key, array.as_fixed_size_binary().value(row)),
        _ => return false,
    }

    true
}

pub fn append_encoded(key: &mut Vec<u8>, encoded: &[u8]) {
    // a batch may lack the column (native null) or hold xml where another holds a string: both must key alike
    if encoded == b"\0n" {
        return key.extend_from_slice(b"\0n");
    }

    if let Some(text) = encoded_string(encoded) {
        return append_bytes(key, text);
    }

    key.extend_from_slice(b"\0z");
    key.extend_from_slice(&(encoded.len() as u64).to_le_bytes());
    key.extend_from_slice(encoded);
}

/// KeyEncoder's `\0s<length>:<bytes>`, the tag it gives every string, xml and html text included.
fn encoded_string(encoded: &[u8]) -> Option<&[u8]> {
    let rest = encoded.strip_prefix(b"\0s")?;
    let colon = rest.iter().position(|&byte| byte == b':')?;
    let length: usize = std::str::from_utf8(&rest[..colon]).ok()?.parse().ok()?;
    let text = &rest[colon + 1..];

    (text.len() == length).then_some(text)
}

fn append_int(key: &mut Vec<u8>, value: i64) {
    key.extend_from_slice(b"\0i");
    key.extend_from_slice(&value.to_le_bytes());
}

fn append_float(key: &mut Vec<u8>, value: f64) {
    // ComparableValues::equality() turns NaN into the string 'NaN'; -0.0 === 0.0
    if value.is_nan() {
        return append_bytes(key, b"NaN");
    }

    key.extend_from_slice(b"\0d");
    key.extend_from_slice(&(if value == 0.0 { 0.0_f64 } else { value }).to_bits().to_le_bytes());
}

fn append_bytes(key: &mut Vec<u8>, bytes: &[u8]) {
    key.extend_from_slice(b"\0s");
    key.extend_from_slice(&(bytes.len() as u64).to_le_bytes());
    key.extend_from_slice(bytes);
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{
        ArrayRef, BooleanArray, Date32Array, DurationMicrosecondArray, FixedSizeBinaryArray, Float64Array, Int64Array,
        LargeBinaryArray, ListArray, NullArray, TimestampMicrosecondArray,
    };
    use std::sync::Arc;

    fn keys(columns: &[ArrayRef]) -> Vec<Vec<u8>> {
        (0..columns[0].len())
            .map(|row| {
                let mut key = Vec::new();

                for column in columns {
                    assert!(append_native_key(&mut key, column.as_ref(), row));
                }

                key
            })
            .collect()
    }

    fn key(column: ArrayRef, row: usize) -> Vec<u8> {
        keys(&[column]).swap_remove(row)
    }

    #[test]
    fn zero_and_negative_zero_are_one_key() {
        let k = keys(&[Arc::new(Float64Array::from(vec![0.0, -0.0, 1.5]))]);

        assert_eq!(k[0], k[1]);
        assert_ne!(k[0], k[2]);
    }

    #[test]
    fn nans_are_one_key_equal_to_the_string_nan() {
        let k = keys(&[Arc::new(Float64Array::from(vec![f64::NAN, -f64::NAN, 1.0]))]);

        assert_eq!(k[0], k[1]);
        assert_ne!(k[0], k[2]);
        assert_eq!(k[0], key(Arc::new(LargeBinaryArray::from_vec(vec![b"NaN"])), 0));
    }

    #[test]
    fn null_is_neither_empty_bytes_nor_zero_nor_false() {
        let strings = keys(&[Arc::new(LargeBinaryArray::from_opt_vec(vec![
            None,
            Some(b""),
            Some(b"0"),
        ]))]);

        assert_ne!(strings[0], strings[1]);
        assert_ne!(strings[0], strings[2]);
        assert_eq!(strings[0], key(Arc::new(Int64Array::from(vec![None::<i64>])), 0));
        assert_eq!(strings[0], key(Arc::new(NullArray::new(1)), 0));
        assert_ne!(strings[0], key(Arc::new(Int64Array::from(vec![0])), 0));
        assert_ne!(strings[0], key(Arc::new(BooleanArray::from(vec![false])), 0));
    }

    #[test]
    fn bools_and_ints_keep_their_own_tags() {
        let bools = keys(&[Arc::new(BooleanArray::from(vec![true, false, true]))]);

        assert_eq!(bools[0], bools[2]);
        assert_ne!(bools[0], bools[1]);
        assert_ne!(bools[0], key(Arc::new(Int64Array::from(vec![1])), 0));
        assert_ne!(
            key(Arc::new(Int64Array::from(vec![1])), 0),
            key(Arc::new(Float64Array::from(vec![1.0])), 0)
        );
    }

    #[test]
    fn a_two_column_key_never_runs_one_value_into_the_next() {
        let k = keys(&[
            Arc::new(LargeBinaryArray::from_vec(vec![b"ab", b"a"])),
            Arc::new(LargeBinaryArray::from_vec(vec![b"", b"b"])),
        ]);

        assert_ne!(k[0], k[1]);
    }

    #[test]
    fn a_date_and_a_datetime_at_the_same_instant_are_one_key() {
        let date = key(Arc::new(Date32Array::from(vec![19_723])), 0);
        let datetime = TimestampMicrosecondArray::from(vec![19_723 * DAY_MICROSECONDS]).with_timezone("UTC");

        assert_eq!(date, key(Arc::new(datetime), 0));
        assert_eq!(
            date,
            key(Arc::new(Int64Array::from(vec![19_723 * DAY_MICROSECONDS])), 0)
        );
        assert_eq!(
            key(Arc::new(DurationMicrosecondArray::from(vec![5])), 0),
            key(Arc::new(Int64Array::from(vec![5])), 0)
        );
    }

    #[test]
    fn a_date_past_i64_microseconds_is_a_float_instant() {
        let far = key(Arc::new(Date32Array::from(vec![i32::MAX])), 0);

        assert_eq!(
            far,
            key(
                Arc::new(Float64Array::from(vec![f64::from(i32::MAX) * DAY_MICROSECONDS as f64])),
                0
            )
        );
    }

    #[test]
    fn a_uuid_is_its_sixteen_physical_bytes() {
        let bytes = [7_u8; 16];
        let uuid = FixedSizeBinaryArray::try_from_iter(vec![bytes].into_iter()).unwrap();

        assert_eq!(
            key(Arc::new(uuid), 0),
            key(Arc::new(LargeBinaryArray::from_vec(vec![&bytes[..]])), 0)
        );
    }

    #[test]
    fn any_other_encoded_part_never_equals_a_native_one() {
        let mut encoded = Vec::new();
        append_encoded(&mut encoded, b"a");

        assert_ne!(encoded, key(Arc::new(LargeBinaryArray::from_vec(vec![b"a"])), 0));
    }

    #[test]
    fn an_encoded_null_and_string_key_as_native_ones_do() {
        let mut null = Vec::new();
        append_encoded(&mut null, b"\0n");
        let mut text = Vec::new();
        append_encoded(&mut text, b"\0s3:a:b");

        assert_eq!(null, key(Arc::new(NullArray::new(1)), 0));
        assert_eq!(text, key(Arc::new(LargeBinaryArray::from_vec(vec![b"a:b"])), 0));
    }

    #[test]
    fn a_list_has_no_native_key() {
        let list = ListArray::from_iter_primitive::<Int64Type, _, _>(vec![Some(vec![Some(1)])]);

        assert!(!append_native_key(&mut Vec::new(), &list, 0));
    }
}
