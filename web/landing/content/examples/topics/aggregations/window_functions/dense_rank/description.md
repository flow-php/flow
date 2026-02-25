The `dense_rank()` function assigns a rank to each row within a partition, without gaps in ranking when there are ties. If two rows have the same value, they get the same rank, but the next rank is consecutive (no gaps).

For example, if two employees have the same salary and both get rank 2, the next employee gets rank 3 (not 4 like with regular `rank()`).
