# Sort

[DOC_LINK:/documentation/components/core/core.md]

## Order

Nulls sort first ascending and last descending, NaN after every other number, strings by bytes; rows with equal keys
keep their input order. Both algorithms below produce the same order.

```php
<?php

data_frame()
    ->read(from_array([['code' => '9'], ['code' => null], ['code' => '10']]))
    ->sortBy([ref('code')->asc()]) // null, '10', '9'
    ->run();
```

Each reference can move its nulls to the other end:

```php
<?php

use Flow\ETL\Row\NullsOrder;

data_frame()
    ->read(from_array([['code' => '9'], ['code' => null], ['code' => '10']]))
    ->sortBy([ref('code')->asc(NullsOrder::LAST)]) // '10', '9', null
    ->run();

data_frame()
    ->read(from_array([['code' => '9'], ['code' => null], ['code' => '10']]))
    ->sortBy([ref('code')->desc(NullsOrder::FIRST)]) // null, '9', '10'
    ->run();
```

## Sort Algorithm

The sort algorithm is configured through `config_builder()->sort()` - all options live on the algorithm's own builder:

- `external_sort()` - **default**; sorts in memory while the buffered rows stay under its memory limit. Past the limit
  it spills each sorted run as a bucket through a `BucketsStorage` (the same abstraction used by
  [join](/documentation/components/core/join.md) and
  [group by](/documentation/components/core/group-by.md)), then k-way merges the runs back into one sorted stream.
  The memory limit defaults to `FLOW_MAX_MEMORY` when set, otherwise 70% of PHP's `memory_limit` (1 GiB when
  `memory_limit` is `-1`), and is compared with the whole process - a sort and a join in one pipeline stay under it
  together.
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
| `FilesystemBuckets` - **Default** | Sorted runs are spilled to disk (Floe files).                                                |
| `MemoryBuckets`                   | Runs are kept in memory - prefer `memory_sort()` instead, it sorts once and skips the merge. |
| `PSRCacheBuckets`                 | Runs are spilled into any PSR-16 cache.                                                      |

```php
<?php

data_frame(
    config_builder()
        ->sort(
            external_sort()
                ->memoryLimit(Unit::fromMb(256))  // process memory past which sorted runs are spilled
                ->bucketsCount(100)               // how many runs are merged at once (merge fan-in)
                ->batchSize(1000)                 // rows per output batch
        )
)
    ->read(from_parquet('orders.parquet'))
    ->sortBy([ref('total')->desc()])
    ->run();
```

The storage implementation is swapped with `external_sort()->storage(BucketsStorage $storage)`, which covers
both phases - the sorted runs and the runs written while merging.

`sortBy()` followed by `limit(n)` keeps only the top `n` rows while it reads. Past the same memory limit it hands
the rows it kept and the rest of the stream to the external sort, whose merge stops after `n` rows.

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
    ->mergeStorage(new FilesystemBuckets($fs, path('/tmp/merge')));  // merged runs only
```

## Example

```php
<?php 

data_frame()
    ->read(from_sequence_number('id', 1, 10))
    ->sortBy([ref('id')->desc()])
    ->collect()
    ->write(to_output(false))
    ->run();
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