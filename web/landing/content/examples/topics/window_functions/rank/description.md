The `rank()` function assigns a rank to each row within a partition, with gaps in ranking when there are ties. If two rows have the same value, they get the same rank, and the next rank is skipped.

For example, if two employees have the same salary and both get rank 2, the next employee gets rank 4 (rank 3 is skipped).
