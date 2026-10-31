`withCorruptedRecords()` sends every corrupted record to a loader as a row (`uri`, `offset`, `line`, `reason`,
`record`, `fields`) and the read goes on. The clean rows are written, the rejects are read back and grouped by reason.
