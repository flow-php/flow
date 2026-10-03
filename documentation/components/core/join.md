# Join

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Joining two data frames is a common operation in data processing that combines data from two different sources. Flow PHP
implements joins using a **hash join algorithm** that creates a hash table from the right DataFrame and probes it with
rows from the left DataFrame. The right DataFrame becomes part of the left one's plan when `join()` / `crossJoin()` is
called, and runs with the left DataFrame's configuration and error handler.

## Join Methods

### join ()

Main join method: builds a hash table from the right DataFrame and probes it with left rows.

### crossJoin () - Cartesian Product

Joins each row from the left side with each row on the right side, creating `count(left) * count(right)` rows total.

### joinEach () - Streaming Join

The right side is built for each left batch by `DataFrameFactory::from(Rows $rows)`, useful for large right-side
datasets that don't fit in memory.

## Join Types

Flow PHP supports four join types with specific behaviors:

| Join Type                              | Description                                                                                |
|----------------------------------------|--------------------------------------------------------------------------------------------|
| **Left Join** (`Join::left`)           | Returns all rows from left DataFrame with matching rows from right (or NULL) - **Default** |
| **Inner Join** (`Join::inner`)         | Returns only rows that exist in both DataFrames                                            |
| **Right Join** (`Join::right`)         | Returns all rows from right DataFrame with matching rows from left (or NULL)               |
| **Left Anti Join** (`Join::left_anti`) | Returns rows from left DataFrame that have NO match in right DataFrame                     |

Joins follow SQL semantics - every matching pair of rows produces one output row, so a left row that matches multiple
right rows is emitted multiple times. A `null` join key never matches anything, including another `null` - rows with
`null` keys are dropped by inner joins and null-padded (or kept, for `left_anti`) by outer joins.

> Rows are hashed by the values of the join columns and every candidate pair is verified against
> the join expression, so non-equality expressions (like `compare_any()`) are supported as well.
> Mixed expressions still hash by their equality conditions - in `compare_all(Equal, Any)` rows are
> hashed by the `Equal` columns and the `Any` part is verified per candidate pair; only joins with
> no equality condition at all fall back to comparing every pair.

## Buckets Storage

While the process stays under the memory limit, `join()` holds the right DataFrame in memory and streams the left one
through its hash table, so left rows keep their order. Past the limit both sides are partitioned by join key into
buckets of a `BucketsStorage` - the same abstraction used by external sort - and joined bucket pair by bucket pair:

| Buckets storage                   | Past the memory limit                                                                                                                       |
|-----------------------------------|---------------------------------------------------------------------------------------------------------------------------------------------|
| `FilesystemBuckets` - **Default** | Both sides are spilled to disk (Floe files); the hash table is built from the smaller bucket of each pair. Left row order is not preserved. |
| `PSRCacheBuckets`                 | Like `FilesystemBuckets`, with buckets in any PSR-16 cache.                                                                                 |
| `MemoryBuckets`                   | Never partitions: the right DataFrame stays in memory whatever its size and left rows keep their order.                                     |

`join()` takes an optional trailing `JoinAlgorithmBuilder`, so one join can override the configured algorithm:

```php ignore
->join($right, join_on(['id' => 'id']), Join::left, hash_join()->storage(new MemoryBuckets()))
```

`joinEach()` does not take one - it builds a DataFrame per left batch, so a per-call algorithm would be rebuilt per
batch too.

The join algorithm is configured through `config_builder()->join(hash_join())` - all its options live on the
`hash_join()` builder. `memoryLimit()` defaults to `FLOW_MAX_MEMORY`, otherwise 70% of PHP's `memory_limit` (1 GB
when it is `-1`), and is compared with the whole process, so a join and a sort in one pipeline stay under it together:

```php
<?php

data_frame(
    config_builder()
        ->join(
            hash_join()
                ->memoryLimit(Unit::fromMb(512))  // process memory past which rows go to the storage
                ->bucketsCount(64)                // number of buckets
                ->batchSize(1000)                 // rows per bucket frame and per batch read back
        )
)
    ->read(from_parquet('orders.parquet'))
    ->join(
        data_frame()->read(from_parquet('sellers.parquet')),
        join_on(['seller_id' => 'id'], join_prefix: 'joined_'),
    )
    ->run();
```


## Example

```php
<?php

$externalProducts = [
    ['id' => 1, 'sku' => 'PRODUCT01'],
    ['id' => 2, 'sku' => 'PRODUCT02'],
    ['id' => 3, 'sku' => 'PRODUCT03'],
];

$internalProducts = [
    ['id' => 2, 'sku' => 'PRODUCT02'],
    ['id' => 3, 'sku' => 'PRODUCT03'],
];

data_frame()
    ->read(from_array($externalProducts))
    ->join(
        data_frame()->read(from_array($internalProducts)),
        Expression::on(['id' => 'id']),
        Join::left_anti
    )
    ->write(to_output())
    ->run();
```

Output:

```console
+----+-----------+
| id |       sku |
+----+-----------+
|  1 | PRODUCT01 |
+----+-----------+
1 rows
```

