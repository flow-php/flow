`from_pgsql_cursor()` reads through a server-side `DECLARE CURSOR`: the result set never leaves
PostgreSQL, rows arrive in batches, and memory stays flat no matter how large the query.
