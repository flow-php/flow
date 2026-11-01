//! Arithmetic over null-free columns with Flow's PHP semantics.

use arrow_arith::arity::binary;
use arrow_arith::numeric;
use arrow_array::cast::AsArray;
use arrow_array::types::{Float64Type, Int64Type};
use arrow_array::{Array, Float64Array, Int64Array};
use arrow_schema::ArrowError;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Operator {
    Add,
    Subtract,
    Multiply,
}

// None on an overflow, which PHP answers with its own error at the first overflowing row
pub fn ints(left: &Int64Array, operator: Operator, right: &Int64Array) -> Option<Int64Array> {
    let result = match operator {
        Operator::Add => numeric::add(left, right),
        Operator::Subtract => numeric::sub(left, right),
        Operator::Multiply => numeric::mul(left, right),
    };

    result.ok().map(|array| array.as_primitive::<Int64Type>().clone())
}

pub fn floats(left: &Float64Array, operator: Operator, right: &Float64Array) -> Result<Float64Array, ArrowError> {
    let result = match operator {
        Operator::Add => numeric::add(left, right),
        Operator::Subtract => numeric::sub(left, right),
        Operator::Multiply => numeric::mul(left, right),
    }?;

    Ok(result.as_primitive::<Float64Type>().clone())
}

// None unless every modulus is positive: PHP refuses zero and negative ones with its own error
pub fn modulo(left: &Int64Array, right: &Int64Array) -> Option<Int64Array> {
    if right.null_count() > 0 || right.values().iter().any(|modulus| *modulus <= 0) {
        return None;
    }

    binary(left, right, |value, modulus| {
        let remainder = value % modulus;

        if remainder < 0 {
            remainder + modulus
        } else {
            remainder
        }
    })
    .ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn modulo_folds_a_negative_remainder_back() {
        assert_eq!(
            modulo(
                &Int64Array::from(vec![7, -7, i64::MIN, 0]),
                &Int64Array::from(vec![3, 3, 3, 5])
            ),
            Some(Int64Array::from(vec![1, 2, 1, 0]))
        );
    }

    #[test]
    fn modulo_leaves_a_zero_or_negative_modulus_to_php() {
        assert_eq!(modulo(&Int64Array::from(vec![1]), &Int64Array::from(vec![0])), None);
        assert_eq!(modulo(&Int64Array::from(vec![1]), &Int64Array::from(vec![-3])), None);
    }

    #[test]
    fn int_arithmetic_leaves_an_overflow_to_php() {
        let max = Int64Array::from(vec![i64::MAX]);
        let one = Int64Array::from(vec![1]);

        assert_eq!(ints(&max, Operator::Add, &one), None);
        assert_eq!(ints(&Int64Array::from(vec![i64::MIN]), Operator::Subtract, &one), None);
        assert_eq!(ints(&max, Operator::Multiply, &Int64Array::from(vec![2])), None);
        assert_eq!(
            ints(&max, Operator::Subtract, &one),
            Some(Int64Array::from(vec![i64::MAX - 1]))
        );
    }

    #[test]
    fn float_arithmetic_is_ieee() {
        let result = floats(
            &Float64Array::from(vec![f64::INFINITY, -0.0, 1.5]),
            Operator::Add,
            &Float64Array::from(vec![f64::NEG_INFINITY, 0.0, f64::NAN]),
        )
        .unwrap();

        assert!(result.value(0).is_nan());
        assert_eq!(result.value(1).to_bits(), 0.0_f64.to_bits());
        assert!(result.value(2).is_nan());
    }
}
