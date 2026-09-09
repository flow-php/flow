`from_dbal_queries()` runs one SQL statement once per parameter set and merges the results into a
single stream - the way to fan a read out across tenants, date ranges or partitions.
