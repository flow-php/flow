A window function computes over related rows without collapsing them: every row stays and gains a
value. `window()` defines the partitions and their order, and `over()` applies `rank()`,
`dense_rank()`, `row_number()` or an aggregate to it.
