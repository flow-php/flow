# Group By

[DOC_LINK:/documentation/components/core/core.md]

`DataFrame::groupBy()` returns a `GroupedDataFrame`; `aggregate()` turns it back into a `DataFrame` with one row per
group:

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
|     A |        3 |
|     B |        2 |
+-------+----------+
2 rows
```

[Aggregations](/documentation/components/core/aggregations.md)

## Buckets Storage

While the process stays under the memory limit, `groupBy()` aggregates in one pass and writes nothing. Past the limit it
partitions the rest of the stream by the hashed grouping key into buckets of a `BucketsStorage` - the same abstraction
used by [external sort](/documentation/components/core/sort.md) and [join](/documentation/components/core/join.md) -
aggregates each bucket on its own and merges the groups aggregated before the limit into them. The default storage is
`FilesystemBuckets` (Floe files in the local cache directory); `MemoryBuckets` and `PSRCacheBuckets` (any PSR-16
cache) are the alternatives.

`groupBy()` takes an optional trailing `GroupByAlgorithmBuilder`, so one operation can override the configured
algorithm:

```php ignore
->groupBy([ref('country')], hash_group_by()->storage(new MemoryBuckets()))
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

Before aggregating, the batch is pruned to the columns the group by reads: the grouping references plus every column
referenced by the aggregators. A grouping or aggregated column the input does not have throws
`SchemaDefinitionNotFoundException`.

Custom aggregators take part through `AggregatingFunction::references()`: return the references the aggregator reads,
or `null` when they cannot be listed - `null` turns pruning off for that group by.

### Output order

Group order is not a contract - past the memory limit groups come out in bucket order. For a specific
order, apply [sortBy](/documentation/components/core/sort.md) after aggregation.