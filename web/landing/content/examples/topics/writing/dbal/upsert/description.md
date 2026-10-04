`sqlite_insert_options(conflict_columns:)` turns the insert into an upsert, so writing the same ten
orders twice leaves ten rows instead of failing. MySQL and PostgreSQL have their own
`*_insert_options()`.
