Keyset pagination with `from_dbal_key_set_qb()`. Each page carries the last row's key forward
instead of counting rows to skip, so cost stays flat as the offset grows.
