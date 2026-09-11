`skip_rows_handler()` drops the whole batch the failure landed in, not just the offending row.
With `batchSize(2)`, losing id 2 loses id 1 with it.
