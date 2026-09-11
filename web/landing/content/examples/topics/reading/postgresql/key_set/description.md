Keyset pagination over a native PostgreSQL connection. `pgsql_pagination_key_set()` names the
columns that carry the cursor; the extractor turns them into an indexed `WHERE` on every page.
