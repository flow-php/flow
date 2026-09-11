`from_dbal_limit_offset()` walks a whole table page by page, ordered by a column you choose.
The simplest extractor - fine until the offset grows large, then reach for keyset pagination.
