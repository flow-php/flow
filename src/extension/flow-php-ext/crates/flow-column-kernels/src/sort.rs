//! Stable sort permutations with Flow's PHP order.

use arrow_array::{Float64Array, UInt32Array};
use arrow_ord::sort::{lexsort_to_indices, LexicographicalComparator};
pub use arrow_ord::sort::{SortColumn, SortOptions};
use arrow_schema::ArrowError;

// arrow sorts unstably, so every run of equal keys is re-sorted by row index to keep the input order among ties
pub fn stable_indices(columns: &[SortColumn]) -> Result<UInt32Array, ArrowError> {
    let mut indices = lexsort_to_indices(columns, None)?.values().to_vec();
    let comparator = LexicographicalComparator::try_new(columns)?;
    let mut run_start = 0;

    for position in 1..=indices.len() {
        let run_ends = position == indices.len()
            || comparator
                .compare(indices[run_start] as usize, indices[position] as usize)
                .is_ne();

        if run_ends {
            indices[run_start..position].sort_unstable();
            run_start = position;
        }
    }

    Ok(UInt32Array::from(indices))
}

// arrow's total order puts -0.0 below 0.0 and a negative NaN first; PHP ties both zeros and ranks every NaN highest
pub fn php_ordered_floats(values: &Float64Array) -> Float64Array {
    values.unary(|value| {
        if value == 0.0 {
            0.0
        } else if value.is_nan() {
            f64::NAN
        } else {
            value
        }
    })
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_array::{ArrayRef, Int64Array, StringArray};

    use super::*;

    fn sorted(columns: Vec<(ArrayRef, bool, bool)>) -> Vec<u32> {
        let columns: Vec<SortColumn> = columns
            .into_iter()
            .map(|(values, descending, nulls_first)| SortColumn {
                values,
                options: Some(SortOptions {
                    descending,
                    nulls_first,
                }),
            })
            .collect();

        stable_indices(&columns).unwrap().values().to_vec()
    }

    #[test]
    fn ties_keep_their_input_order_in_both_directions() {
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![2, 1, 2, 1, 2]));

        assert_eq!(sorted(vec![(keys.clone(), false, true)]), [1, 3, 0, 2, 4]);
        assert_eq!(sorted(vec![(keys, true, false)]), [0, 2, 4, 1, 3]);
    }

    #[test]
    fn ties_at_the_first_and_the_last_row_keep_their_order() {
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![5, 5, 3, 7, 7]));

        assert_eq!(sorted(vec![(keys, false, true)]), [2, 0, 1, 3, 4]);
    }

    #[test]
    fn every_row_equal_is_the_input_order() {
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![1; 50]));

        assert_eq!(sorted(vec![(keys, true, false)]), (0..50).collect::<Vec<u32>>());
    }

    #[test]
    fn ties_under_mixed_directions_keep_their_order() {
        let groups: ArrayRef = Arc::new(StringArray::from(vec!["b", "a", "b", "a", "b"]));
        let scores: ArrayRef = Arc::new(Int64Array::from(vec![1, 2, 1, 2, 3]));

        assert_eq!(
            sorted(vec![(groups, false, true), (scores, true, false)]),
            [1, 3, 4, 0, 2]
        );
    }

    #[test]
    fn nulls_go_where_the_column_puts_them() {
        let keys: ArrayRef = Arc::new(Int64Array::from(vec![Some(2), None, Some(1)]));

        assert_eq!(sorted(vec![(keys.clone(), false, true)]), [1, 2, 0]);
        assert_eq!(sorted(vec![(keys, false, false)]), [2, 0, 1]);
    }

    #[test]
    fn floats_tie_both_zeros_and_rank_every_nan_highest() {
        let floats = php_ordered_floats(&Float64Array::from(vec![f64::NAN, -0.0, 0.0, -f64::NAN, f64::INFINITY]));

        assert_eq!(sorted(vec![(Arc::new(floats), false, true)]), [1, 2, 4, 0, 3]);
    }
}
