`to_pgsql_transaction()` is a transaction root grouping sinks: every batch is written in its own transaction,
rolled back if any sink fails; rows a sink's `Transformation` delivers when the run ends commit in one final
transaction. Every sink's loader must use the same `Client` instance as the transaction - a loader holding its own
`Client` escapes it.
