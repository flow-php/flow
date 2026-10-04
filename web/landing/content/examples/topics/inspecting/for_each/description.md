`forEach()` runs the pipeline and hands each batch (`Rows`) to a callback. A batch is read by
column - `$rows->column('name')->values()` - or as rows with `toArray()`.
