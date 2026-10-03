`ignore_error_handler()` drops the batch a transformation failed in and continues with the next one.
With `batchSize(1)` that is only the failing row. Unlike `skip_rows_handler()`, it also survives a
failing loader: that loader misses the batch, the other writes still run.
