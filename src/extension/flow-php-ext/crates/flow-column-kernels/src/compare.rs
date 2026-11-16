//! Comparisons, text matches and null tests with Flow's PHP semantics: a result is null where either side is null.

use arrow_array::types::Float64Type;
use arrow_array::{Array, ArrayRef, BooleanArray, Datum, Float64Array, Int64Array, LargeBinaryArray};
use arrow_buffer::{BooleanBuffer, NullBuffer};
use arrow_ord::cmp;
use arrow_schema::ArrowError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Equal,
    NotEqual,
    LessThan,
    LessThanOrEqual,
    GreaterThan,
    GreaterThanOrEqual,
}

pub fn compare_ordered(left: &ArrayRef, operator: Operator, right: &ArrayRef) -> Result<BooleanArray, ArrowError> {
    let (left, right): (&dyn Datum, &dyn Datum) = (left, right);

    match operator {
        Operator::Equal => cmp::eq(left, right),
        Operator::NotEqual => cmp::neq(left, right),
        Operator::LessThan => cmp::lt(left, right),
        Operator::LessThanOrEqual => cmp::lt_eq(left, right),
        Operator::GreaterThan => cmp::gt(left, right),
        Operator::GreaterThanOrEqual => cmp::gt_eq(left, right),
    }
}

// arrow orders floats by total_cmp (-0.0 < 0.0, a negative NaN first); PHP has -0.0 == 0.0, NaN == NaN, NaN greatest
pub fn compare_floats(left: &Float64Array, operator: Operator, right: &Float64Array) -> BooleanArray {
    let holds = |a: f64, b: f64| match operator {
        Operator::Equal => equal(a, b),
        Operator::NotEqual => !equal(a, b),
        Operator::LessThan => less(a, b),
        Operator::LessThanOrEqual => less_or_equal(a, b),
        Operator::GreaterThan => less(b, a),
        Operator::GreaterThanOrEqual => less_or_equal(b, a),
    };

    BooleanArray::new(
        BooleanBuffer::collect_bool(left.len(), |i| holds(left.value(i), right.value(i))),
        NullBuffer::union(left.nulls(), right.nulls()),
    )
}

// PHP compares an int with a float as (float) $int
pub fn floats_of_ints(ints: &Int64Array) -> Float64Array {
    ints.unary::<_, Float64Type>(|int| int as f64)
}

pub fn is_null(array: &dyn Array) -> BooleanArray {
    BooleanArray::new(
        array
            .logical_nulls()
            .map_or_else(|| BooleanBuffer::new_unset(array.len()), |nulls| !nulls.inner()),
        None,
    )
}

pub fn is_not_null(array: &dyn Array) -> BooleanArray {
    BooleanArray::new(
        array
            .logical_nulls()
            .map_or_else(|| BooleanBuffer::new_set(array.len()), |nulls| nulls.inner().clone()),
        None,
    )
}

pub fn starts_with(haystacks: &LargeBinaryArray, needles: &LargeBinaryArray) -> BooleanArray {
    matched(haystacks, needles, |haystack, needle| haystack.starts_with(needle))
}

pub fn ends_with(haystacks: &LargeBinaryArray, needles: &LargeBinaryArray) -> BooleanArray {
    matched(haystacks, needles, |haystack, needle| haystack.ends_with(needle))
}

pub fn contains(haystacks: &LargeBinaryArray, needles: &LargeBinaryArray) -> BooleanArray {
    matched(haystacks, needles, |haystack, needle| {
        memchr::memmem::find(haystack, needle).is_some()
    })
}

// the caller passes columns of one length
fn matched(
    haystacks: &LargeBinaryArray,
    needles: &LargeBinaryArray,
    holds: impl Fn(&[u8], &[u8]) -> bool,
) -> BooleanArray {
    BooleanArray::new(
        BooleanBuffer::collect_bool(haystacks.len(), |i| holds(haystacks.value(i), needles.value(i))),
        NullBuffer::union(haystacks.nulls(), needles.nulls()),
    )
}

fn equal(a: f64, b: f64) -> bool {
    a == b || (a.is_nan() && b.is_nan())
}

fn less(a: f64, b: f64) -> bool {
    if b.is_nan() {
        return !a.is_nan();
    }

    !a.is_nan() && a < b
}

fn less_or_equal(a: f64, b: f64) -> bool {
    b.is_nan() || (!a.is_nan() && a <= b)
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{FixedSizeBinaryArray, NullArray};

    use super::*;

    const OPERATORS: [Operator; 6] = [
        Operator::Equal,
        Operator::NotEqual,
        Operator::LessThan,
        Operator::LessThanOrEqual,
        Operator::GreaterThan,
        Operator::GreaterThanOrEqual,
    ];

    fn floats(left: f64, right: f64) -> Vec<bool> {
        OPERATORS
            .iter()
            .map(|operator| {
                compare_floats(
                    &Float64Array::from(vec![left]),
                    *operator,
                    &Float64Array::from(vec![right]),
                )
                .value(0)
            })
            .collect()
    }

    #[test]
    fn negative_zero_equals_zero() {
        assert_eq!(floats(-0.0, 0.0), [true, false, false, true, false, true]);
    }

    #[test]
    fn nan_equals_nan() {
        assert_eq!(floats(f64::NAN, f64::NAN), [true, false, false, true, false, true]);
        assert_eq!(floats(-f64::NAN, f64::NAN), [true, false, false, true, false, true]);
    }

    #[test]
    fn nan_is_the_greatest_float() {
        assert_eq!(floats(f64::NAN, f64::INFINITY), [false, true, false, false, true, true]);
        assert_eq!(floats(-f64::NAN, 1.0), [false, true, false, false, true, true]);
        assert_eq!(floats(1.0, f64::NAN), [false, true, true, true, false, false]);
    }

    #[test]
    fn floats_order_by_value() {
        assert_eq!(floats(1.5, 2.5), [false, true, true, true, false, false]);
    }

    #[test]
    fn a_float_comparison_is_null_where_either_side_is_null() {
        let compared = compare_floats(
            &Float64Array::from(vec![Some(1.0), None, Some(1.0)]),
            Operator::Equal,
            &Float64Array::from(vec![Some(1.0), Some(1.0), None]),
        );

        assert_eq!(compared, BooleanArray::from(vec![Some(true), None, None]));
    }

    #[test]
    fn an_int_compares_as_its_float() {
        let floats = floats_of_ints(&Int64Array::from(vec![Some(9_007_199_254_740_993), None]));

        assert_eq!(floats.value(0), 9_007_199_254_740_992.0);
        assert!(floats.is_null(1));
    }

    #[test]
    fn bytes_order_by_their_bytes() {
        let left: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"a"[..]), Some(b"B"), None]));
        let right: ArrayRef = Arc::new(LargeBinaryArray::from(vec![Some(&b"b"[..]), Some(b"a"), Some(b"a")]));

        assert_eq!(
            compare_ordered(&left, Operator::LessThan, &right).unwrap(),
            BooleanArray::from(vec![Some(true), Some(true), None])
        );
    }

    #[test]
    fn uuids_order_by_their_bytes() {
        let left: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter([[1_u8; 16], [2; 16]].into_iter()).unwrap());
        let right: ArrayRef = Arc::new(FixedSizeBinaryArray::try_from_iter([[2_u8; 16], [2; 16]].into_iter()).unwrap());

        assert_eq!(
            compare_ordered(&left, Operator::LessThanOrEqual, &right).unwrap(),
            BooleanArray::from(vec![true, true])
        );
        assert_eq!(
            compare_ordered(&left, Operator::Equal, &right).unwrap(),
            BooleanArray::from(vec![false, true])
        );
    }

    #[test]
    fn ints_compare_with_nulls_unknown() {
        let left: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MIN), None]));
        let right: ArrayRef = Arc::new(Int64Array::from(vec![Some(i64::MAX), Some(1)]));

        assert_eq!(
            compare_ordered(&left, Operator::GreaterThanOrEqual, &right).unwrap(),
            BooleanArray::from(vec![Some(false), None])
        );
    }

    #[test]
    fn null_tests_read_the_validity() {
        let ints = Int64Array::from(vec![Some(1), None]);

        assert_eq!(is_null(&ints), BooleanArray::from(vec![false, true]));
        assert_eq!(is_not_null(&ints), BooleanArray::from(vec![true, false]));
    }

    #[test]
    fn null_tests_of_a_column_without_validity() {
        let ints = Int64Array::from(vec![1, 2]);

        assert_eq!(is_null(&ints), BooleanArray::from(vec![false, false]));
        assert_eq!(is_not_null(&ints), BooleanArray::from(vec![true, true]));
    }

    #[test]
    fn every_value_of_a_null_column_is_null() {
        let nulls = NullArray::new(2);

        assert_eq!(is_null(&nulls), BooleanArray::from(vec![true, true]));
        assert_eq!(is_not_null(&nulls), BooleanArray::from(vec![false, false]));
    }

    fn texts(values: &[Option<&[u8]>]) -> LargeBinaryArray {
        LargeBinaryArray::from(values.to_vec())
    }

    #[test]
    fn a_text_match_is_null_where_either_side_is_null() {
        let haystacks = texts(&[Some(b"abc"), None, Some(b"abc")]);
        let expected = BooleanArray::from(vec![Some(true), None, None]);

        assert_eq!(
            starts_with(&haystacks, &texts(&[Some(b"a"), Some(b"a"), None])),
            expected
        );
        assert_eq!(ends_with(&haystacks, &texts(&[Some(b"c"), Some(b"c"), None])), expected);
        assert_eq!(contains(&haystacks, &texts(&[Some(b"b"), Some(b"b"), None])), expected);
    }

    #[test]
    fn an_empty_needle_matches_every_text() {
        let haystacks = texts(&[Some(b"abc"), Some(b"")]);
        let needles = texts(&[Some(b""), Some(b"")]);
        let expected = BooleanArray::from(vec![true, true]);

        assert_eq!(starts_with(&haystacks, &needles), expected);
        assert_eq!(ends_with(&haystacks, &needles), expected);
        assert_eq!(contains(&haystacks, &needles), expected);
    }

    #[test]
    fn a_needle_longer_than_its_text_matches_nothing() {
        let haystacks = texts(&[Some(b"x")]);
        let needles = texts(&[Some(b"xx")]);
        let expected = BooleanArray::from(vec![false]);

        assert_eq!(starts_with(&haystacks, &needles), expected);
        assert_eq!(ends_with(&haystacks, &needles), expected);
        assert_eq!(contains(&haystacks, &needles), expected);
    }

    #[test]
    fn a_text_match_compares_bytes_that_are_not_utf8_and_nul_bytes() {
        let haystacks = texts(&[Some(b"\xffa\x00b\xfe"), Some(b"\xffa\x00b\xfe")]);

        assert_eq!(
            starts_with(&haystacks, &texts(&[Some(b"\xffa\x00"), Some(b"\xfe")])),
            BooleanArray::from(vec![true, false])
        );
        assert_eq!(
            ends_with(&haystacks, &texts(&[Some(b"\x00b\xfe"), Some(b"\xff")])),
            BooleanArray::from(vec![true, false])
        );
        assert_eq!(
            contains(&haystacks, &texts(&[Some(b"a\x00b"), Some(b"\x00\x00")])),
            BooleanArray::from(vec![true, false])
        );
    }

    #[test]
    fn a_text_match_reads_sliced_columns_from_their_offset() {
        let haystacks = texts(&[Some(b"zzz"), Some(b"abc"), None, Some(b"cab")]).slice(1, 3);
        let needles = texts(&[None, Some(b"ab"), Some(b"ab"), Some(b"ab")]).slice(1, 3);

        assert_eq!(
            starts_with(&haystacks, &needles),
            BooleanArray::from(vec![Some(true), None, Some(false)])
        );
        assert_eq!(
            ends_with(&haystacks, &needles),
            BooleanArray::from(vec![Some(false), None, Some(true)])
        );
        assert_eq!(
            contains(&haystacks, &needles),
            BooleanArray::from(vec![Some(true), None, Some(true)])
        );
    }
}
