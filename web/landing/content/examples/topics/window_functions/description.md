Window functions perform calculations across a set of rows that are related to the current row. Unlike regular aggregations, window functions don't collapse rows - each row retains its identity while gaining access to aggregate information about its "window" of related rows.

Use `window()` to define partitions and ordering, then apply window functions like `rank()`, `dense_rank()`, or `row_number()` to compute values within each partition.
