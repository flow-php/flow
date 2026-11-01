# Group By

[DOC_LINK:/documentation/components/core/core.md]

`DataFrame::groupBy()` returns a `GroupedDataFrame`. `aggregate()` turns it back into a `DataFrame` with one row for
each group:

```php
data_frame()
    ->read(from_array([
        ['id' => 1, 'group' => 'A'],
        ['id' => 2, 'group' => 'B'],
        ['id' => 3, 'group' => 'A'],
        ['id' => 4, 'group' => 'B'],
        ['id' => 5, 'group' => 'A'],
    ]))
    ->groupBy([ref('group')])
    ->aggregate(count(ref('id')))
    ->write(to_output(truncate: false))
    ->run();
```

```console
+-------+----------+
| group | id_count |
+-------+----------+
| A | 3 |
| B | 2 |
+-------+----------+
2 rows
```

[Aggregations](/documentation/components/core/aggregations.md)

## Buckets Storage

While the process stays under the memory limit, `groupBy()` aggregates in one pass and writes nothing. After the
limit, it partitions the rest of the stream by the hashed grouping key into buckets of a `RowsStorage`.
[External sort](/documentation/components/core/sort.md) and [join](/documentation/components/core/join.md) use the
same abstraction. It aggregates each bucket separately, and merges the groups from before the limit into them. The
default storage is `FilesystemRowsStorage` (Floe files in the local cache directory). The alternatives are
`MemoryRowsStorage` and `PsrRowsStorage` (each PSR-16 cache).

`groupBy()` takes an optional `BucketingAlgorithmBuilder` as the last argument. One operation can therefore override
the configured algorithm:

```php ignore
->groupBy([ref('country')], hash_group_by()->storage(new MemoryRowsStorage()))
```

The algorithm is configured through `config_builder()->groupBy(hash_group_by())`. `memoryLimit()` defaults to the
same limit as the join and the sort (`FLOW_MAX_MEMORY`, otherwise 70% of `memory_limit`):

```php
<?php

data_frame(
    config_builder()
        ->groupBy(
            hash_group_by()
                ->memoryLimit(Unit::fromMb(512))  // process memory past which buckets go to the storage
                ->bucketsCount(64)                // number of buckets (default 64)
                ->batchSize(1000)                 // rows per bucket frame and per batch read back (default 1000)
        )
)
    ->read(from_parquet('orders.parquet'))
    ->groupBy([ref('country')])
    ->aggregate(sum(ref('total')))
    ->run();
```


### Column pruning

Before the aggregation, the batch is pruned to the columns that the group by reads: the grouping references plus
each column that the aggregators reference. A grouping or aggregated column that the input does not have throws
`SchemaDefinitionNotFoundException`.

Custom aggregators take part through `AggregatingFunction::references()`. It returns the references that the
aggregator reads, or `null` for references that it cannot list. `null` turns off pruning for that group by.

### Output order

The group order is not a contract. After the memory limit, groups come out in bucket order. For a specific order,
apply [sortBy](/documentation/components/core/sort.md) after the aggregation.