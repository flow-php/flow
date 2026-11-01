# Sort

[DOC_LINK:/documentation/components/core/core.md]

## Order

Nulls sort first in ascending order and last in descending order. NaN sorts after all other numbers, and strings sort
by bytes. Rows with equal keys keep their input order. Both algorithms below produce the same order.

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

use Flow\ETL\Values\NullsOrder;

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

Configure the sort algorithm through `config_builder()->sort()`. All options are on the builder of the algorithm:

- `external_sort()`: the default. It sorts in memory while the buffered rows stay under its memory limit. After the
  limit, it spills each sorted run as a bucket through a `RowsStorage`.
  [Join](/documentation/components/core/join.md) and
  [group by](/documentation/components/core/group-by.md) use the same abstraction. Then it k-way merges the runs back
  into one sorted stream. The default memory limit is `FLOW_MAX_MEMORY`. Without it, the limit is 70% of the
  `memory_limit` of PHP, or 1 GB for a `memory_limit` of `-1`. The limit applies to the full process, so a sort and a
  join in one pipeline stay under it together.
- `memory_sort()`: buffers the full dataset and sorts it in one pass. It is the fastest, but everything must fit in
  RAM. It has no options.

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

| Buckets storage                       | Behavior                                                                                     |
|---------------------------------------|----------------------------------------------------------------------------------------------|
| `FilesystemRowsStorage` (default)     | Sorted runs are spilled to disk (Floe files).                                                |
| `MemoryRowsStorage`                   | Runs stay in memory. Use `memory_sort()` instead. It sorts once and skips the merge.         |
| `PsrRowsStorage`                      | Runs are spilled into each PSR-16 cache.                                                     |

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

To change the storage implementation, use `external_sort()->storage(RowsStorage $storage)`. It covers both phases:
the sorted runs and the runs that the merge writes.

`sortBy()` followed by `limit(n)` keeps only the top `n` rows while it reads. After the same memory limit, it gives
the rows that it kept and the rest of the stream to the external sort. The merge of the external sort stops after `n`
rows.

## Overriding the algorithm for one sort

`sortBy()` takes an optional second argument. One sort can therefore use a different algorithm than the
configuration. All other steps in the pipeline keep the configured default:

```php
<?php

data_frame()
    ->read(from_parquet('orders.parquet'))
    ->sortBy([ref('total')->desc()], external_sort()->storage(new MemoryRowsStorage()))
    ->write(to_output())
    ->run();
```

`null`, or no argument, uses the configured algorithm.

To keep merged runs in a different place, add `external_sort()->mergeStorage(RowsStorage $storage)`. Its default
is the spill storage, so `storage()` alone is still sufficient:

```php
external_sort()
    ->storage(new MemoryRowsStorage())                                                       // spill runs
    ->mergeStorage(new FilesystemRowsStorage($fs, path('/tmp/merge'), new AdaptiveBackend())); // merged runs only
```

`FilesystemRowsStorage` reads runs back into the [column backend](/documentation/components/core/column-backend.md) that it gets.

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
| 9  |
| 8  |
| 7  |
| 6  |
| 5  |
| 4  |
| 3  |
| 2  |
| 1  |
+----+
10 rows
```