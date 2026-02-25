The `row_number()` function assigns a unique sequential number to each row within a partition, starting at 1. Unlike `rank()` and `dense_rank()`, `row_number()` always assigns different numbers to each row, even when values are tied.

This is useful for pagination, selecting top N rows per group, or creating unique identifiers within partitions.
