`to_output(output: Output::schema)` prints the schema instead of the rows: a tree with structure
fields nested under their parent. Here the schema is inferred, so a key some rows lack becomes a
nullable column (`?`).
