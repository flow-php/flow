`to_pgsql_table()` inserts rows over a native `ext-pgsql` connection, batching them into multi-row
statements as the DataFrame streams.
