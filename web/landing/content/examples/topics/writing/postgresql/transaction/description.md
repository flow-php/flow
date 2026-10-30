`to_pgsql_transaction()` groups writes that commit together: every batch is written in its own transaction,
rolled back if any write fails; rows a branch's `Transformation` delivers when the run ends commit in one final
transaction. Every child's loader must use the same `Client` instance as the transaction - a loader holding its own
`Client` escapes it.
