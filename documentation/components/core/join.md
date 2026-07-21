# Join

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Joining two data frames is a common operation in data processing that combines data from two different sources. Flow PHP
implements joins using a **hash join algorithm** that creates a hash table from the right DataFrame and probes it with
rows from the left DataFrame.

## Join Methods

### join()

Main join method that partitions the right DataFrame into buckets and probes them with left rows through a hash table.

### crossJoin() - Cartesian Product

Joins each row from the left side with each row on the right side, creating `count(left) * count(right)` rows total.

### joinEach() - Streaming Join

Right side is dynamically generated for each left row, useful for large right-side datasets that don't fit in memory.

## Join Types

Flow PHP supports four join types with specific behaviors:

| Join Type                              | Description                                                                                |
|----------------------------------------|--------------------------------------------------------------------------------------------|
| **Left Join** (`Join::left`)           | Returns all rows from left DataFrame with matching rows from right (or NULL) - **Default** |
| **Inner Join** (`Join::inner`)         | Returns only rows that exist in both DataFrames                                            |
| **Right Join** (`Join::right`)         | Returns all rows from right DataFrame with matching rows from left (or NULL)               |
| **Left Anti Join** (`Join::left_anti`) | Returns rows from left DataFrame that have NO match in right DataFrame                     |

Joins follow SQL semantics - every matching pair of rows produces one output row, so a left row
that matches multiple right rows is emitted multiple times. A `null` join key never matches
anything, including another `null` - rows with `null` keys are dropped by inner joins and
null-padded (or kept, for `left_anti`) by outer joins.

> Flow uses hash join implementation where hashes are stored in buckets to optimize memory usage and performance.
> Rows are bucketed by the values of the join columns and every candidate pair is verified against
> the join expression, so non-equality expressions (like `compare_any()`) are supported as well.
> Mixed expressions still hash by their equality conditions - in `compare_all(Equal, Any)` rows are
> bucketed by the `Equal` columns and the `Any` part is verified per candidate pair; only joins with
> no equality condition at all fall back to comparing every pair.

## Buckets Storage

`join()` always partitions the right DataFrame into buckets through a `BucketsStorage` -
the same abstraction used by external sort - and the configured implementation decides
how the join executes:

| Buckets storage                   | Behavior                                                                                                                                                                                                                                 |
|-----------------------------------|-------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| `FilesystemBuckets` - **Default** | Both sides are partitioned by join key into buckets spilled to disk (Floe files), then joined pair by pair - the hash table is built from the smaller bucket of each pair. Memory usage is bounded by that bucket, left row order is not preserved. |
| `MemoryBuckets`                   | Right DataFrame is held in memory, left rows are streamed through a hash table and keep their order. Memory usage is bounded by the right side.                                                                                            |

The implementation is swapped with `config_builder()->joinCache(BucketsStorage $cache)`; any storage
implementing `ResidentBucketsStorage` (like `MemoryBuckets`) enables the streaming,
order-preserving execution:

```php
<?php

data_frame(
    config_builder()
        ->joinCache(new MemoryBuckets())        // right side fits in memory, keep left row order
        ->joinBucketsCount(64)                  // number of disk buckets
        ->joinBatchSize(1000)                   // rows per batch when reading buckets back
)
    ->read(from_parquet('orders.parquet'))
    ->join(
        data_frame()->read(from_parquet('sellers.parquet')),
        join_on(['seller_id' => 'id'], join_prefix: 'seller_'),
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

