# Group By

[DOC_LINK:/documentation/components/core/core.md]

Grouping is a common operation in data analysis. The concept is no different from the one you know from SQL.

To group data you need to use `DataFrame::groupBy` function. 

Example: 

```php
data_frame()
    ->read(from_array([
        ['id' => 1, 'group' => 'A'],
        ['id' => 2, 'group' => 'B'],
        ['id' => 3, 'group' => 'A'],
        ['id' => 4, 'group' => 'B'],
        ['id' => 5, 'group' => 'A'],
        ['id' => 6, 'group' => 'B'],
        ['id' => 7, 'group' => 'A'],
        ['id' => 8, 'group' => 'B'],
        ['id' => 9, 'group' => 'A'],
        ['id' => 10, 'group' => 'B'],
    ]))
    ->groupBy([ref('group')])
    ->write(to_output(truncate: false))
    ->run();
```

However, the result of this operation is not very useful. It will just return a `DataFrame` with one column `group`:

```console
+-------+
| group |
+-------+
|     A |
|     B |
+-------+
2 rows
```

To make it more useful, you need to use one of the aggregation functions.

[Aggregations](/documentation/components/core/aggregations.md)

## Buckets Storage

`groupBy()` partitions rows by the hashed grouping key into buckets through a `BucketsStorage` - the
same abstraction used by [external sort](/documentation/components/core/sort.md) and
[join](/documentation/components/core/join.md). The default is `FilesystemBuckets`: buckets are
spilled to the local filesystem cache directory (as Floe files), so memory usage is bounded by the
largest bucket instead of the whole grouped dataset. Other implementations are `MemoryBuckets`
(buckets kept in memory) and `PSRCacheBuckets` (buckets in any PSR-16 cache).

`groupBy()` takes an optional trailing `GroupByAlgorithmBuilder`, so one operation can override the configured
algorithm:

```php ignore
->groupBy([ref('country')], hash_group_by()->storage(new MemoryBuckets()))
```

The algorithm is configured through `config_builder()->groupBy(hash_group_by())` - all its options
live on the `hash_group_by()` builder. While the process is under `memoryLimit()` (the same default as the join and
the sort) the input aggregates in one pass and nothing is written; past it the rest of the stream is partitioned into
buckets of the storage, in frames of `batchSize` rows:

```php
<?php

data_frame(
    config_builder()
        ->groupBy(
            hash_group_by()
                ->memoryLimit(Unit::fromMb(512))  // process memory past which buckets go to the storage
                ->bucketsCount(64)                // number of buckets (default 64)
                ->batchSize(1000)                 // rows per bucket frame and per batch read back (default 1000)
                ->storage(new MemoryBuckets())    // keep buckets in memory instead of on disk
        )
)
    ->read(from_parquet('orders.parquet'))
    ->groupBy([ref('country')])
    ->aggregate(sum(ref('total')))
    ->run();
```


### Spill column pruning

Before rows are spilled to buckets, they are pruned to only the columns the group by actually reads:
the grouping references plus every column referenced by the aggregators. A row that is missing one of
those columns is normalized to a `null` entry for it (Spark/DuckDB semantics) - aggregators silently
skip `null` values, and STRICT schema mode does not throw for aggregate columns missing under a
`groupBy()`.

Custom aggregators participate through `AggregatingFunction::references()`: return the references the
aggregator reads, or `null` when they cannot be statically enumerated - returning `null` disables
column pruning for the whole pipeline (rows are spilled with all their columns).

### Output order

Grouped output follows hash-bucket order, not input order. Group order was never a contract - apply
[sortBy](/documentation/components/core/sort.md) after aggregation when a specific order is needed.