# Sort

[DOC_LINK:/documentation/components/core/core.md]

## Sort Algorithm

The sort algorithm is configured through `config_builder()->sort()` - all options live on the algorithm's own builder:

- `external_sort()` - **default**; buffers rows in memory, sorts them and spills each sorted run as a bucket through a
  `BucketsStorage` (the same abstraction used by
  [join](/documentation/components/core/join.md) and
  [group by](/documentation/components/core/group-by.md)), then k-way merges the runs back into one sorted stream.
  Memory usage is bounded by one run.
- `memory_sort()` - buffers the whole dataset and sorts it in one pass; fastest, but everything must fit in RAM. It has
  no options.

```php
<?php

data_frame(
    config_builder()->sort(memory_sort())
)
    ->read(from_parquet('orders.parquet'))
    ->sortBy([ref('total')->desc()])
    ->run();
```

### External sort options

| Buckets storage                   | Behavior                                                                                     |
|-----------------------------------|----------------------------------------------------------------------------------------------|
| `FilesystemBuckets` - **Default** | Sorted runs are spilled to disk (Floe files). Memory usage is bounded by one run.            |
| `MemoryBuckets`                   | Runs are kept in memory - prefer `memory_sort()` instead, it sorts once and skips the merge. |
| `PSRCacheBuckets`                 | Runs are spilled into any PSR-16 cache.                                                      |

```php
<?php

data_frame(
    config_builder()
        ->sort(
            external_sort()
                ->runSize(10_000)               // rows buffered and sorted in memory before spilled as one run
                ->bucketsCount(100)             // how many runs are merged at once (merge fan-in)
                ->batchSize(1000)               // rows per spill/output batch
        )
)
    ->read(from_parquet('orders.parquet'))
    ->sortBy([ref('total')->desc()])
    ->run();
```

The storage implementation is swapped with `external_sort()->storage(BucketsStorage $storage)`, which covers
both phases - the runs spilled during bucketing and the runs written while merging.

## Overriding the algorithm for one sort

`sortBy()` takes an optional second argument, so a single sort can use a different algorithm from the one on the
config - everything else in the pipeline keeps the configured default:

```php
<?php

data_frame()
    ->read(from_parquet('orders.parquet'))
    ->sortBy([ref('total')->desc()], external_sort()->storage(new MemoryBuckets()))
    ->write(to_output())
    ->run();
```

Passing `null` (or omitting it) uses the configured algorithm.

To keep merged runs somewhere else, add `external_sort()->mergeStorage(BucketsStorage $storage)`. It defaults
to the spill storage, so `storage()` alone stays sufficient:

```php
external_sort()
    ->storage(new MemoryBuckets())                                   // spill runs
    ->mergeStorage(new FilesystemBuckets($fs, path('/tmp/merge')))   // merged runs only
```

> While runs are spilled, each bucket is announced downstream as a single metadata row
> (`BucketShape`: `_bucket_id`, `_bucket_total_rows`). Those rows are an internal pipeline detail consumed
> by the merge processor - they never appear in the sorted output.

## Example

```php
<?php 

data_frame()
    ->read(from_sequence_number('id', 1, 10))
    ->sortBy([ref('id')->desc()])
    ->collect()
    ->write(to_output(false))
    ->run()
```

Output:

```console
+----+
| id |
+----+
| 10 |
|  9 |
|  8 |
|  7 |
|  6 |
|  5 |
|  4 |
|  3 |
|  2 |
|  1 |
+----+
10 rows
```