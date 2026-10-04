`batchBy('order_id')` cuts batches only where `order_id` changes, so rows sharing a value always land
in the same batch. The input must already be ordered by that column - `constraint_sorted_by()` fails
the run if it is not.
