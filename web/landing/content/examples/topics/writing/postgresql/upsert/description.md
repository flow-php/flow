Insert or update in one statement. `pgsql_insert_options(conflictColumns:, updateColumns:)` becomes
`ON CONFLICT ... DO UPDATE`, so running the same load twice leaves ten rows, not an error.
