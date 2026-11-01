# Join

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

A join of two data frames is a common data processing operation. It combines data from two different sources. Flow
PHP implements joins with a hash join algorithm. The algorithm creates a hash table from the right DataFrame and
probes it with rows from the left DataFrame. At the `join()` / `crossJoin()` call, the right DataFrame becomes part of
the plan of the left DataFrame. It runs with the configuration and the error handler of the left DataFrame.

## Join Methods

### join ()

Main join method: builds a hash table from the right DataFrame and probes it with left rows.

### crossJoin () - Cartesian Product

Joins each row from the left side with each row on the right side. The total is `count(left) * count(right)` rows.

### joinEach () - Streaming Join

For each left batch, the right side is read from the extractor that `ExtractorFactory::from(Rows $rows)` returns.
Use it for large right-side datasets that do not fit in memory.

## Join Types

Flow PHP supports four join types:

| Join Type                              | Description                                                                           |
|----------------------------------------|---------------------------------------------------------------------------------------|
| Left Join (`JoinType::left`)           | Returns all rows from left DataFrame with matching rows from right (or NULL). Default |
| Inner Join (`JoinType::inner`)         | Returns only rows that exist in both DataFrames                                       |
| Right Join (`JoinType::right`)         | Returns all rows from right DataFrame with matching rows from left (or NULL)          |
| Left Anti Join (`JoinType::left_anti`) | Returns rows from left DataFrame that have NO match in right DataFrame                |

Joins follow SQL semantics. Each matching pair of rows produces one output row. A left row that matches multiple
right rows therefore comes out multiple times. A `null` join key never matches anything, also not another `null`.
Inner joins drop rows with `null` keys. Outer joins pad them with nulls (or keep them, for `left_anti`).

> Rows are hashed by the values of the join columns. Each candidate pair is then tested against the join
> expression, so non-equality expressions (such as `compare_any()`) are also supported.
> Mixed expressions still hash by their equality conditions. In `compare_all(Equal, Any)`, rows are hashed by the
> `Equal` columns, and the `Any` part is tested for each candidate pair. Only joins with no equality condition at all
> compare each pair.

## Buckets Storage

While the process stays under the memory limit, `join()` holds the right DataFrame in memory. It streams the left
DataFrame through its hash table, so left rows keep their order. After the limit, both sides are partitioned by join
key into buckets of a `RowsStorage`. External sort uses the same abstraction. Then the join runs bucket pair by bucket
pair:

| Buckets storage                   | Past the memory limit                                                                                                                  |
|-----------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| `FilesystemRowsStorage` (default) | Both sides are spilled to disk (Floe files). The hash table is built from the smaller bucket of each pair. Left row order is not kept. |
| `PsrRowsStorage`                  | Like `FilesystemRowsStorage`, with buckets in each PSR-16 cache.                                                                       |
| `MemoryRowsStorage`               | Never partitions: the right DataFrame stays in memory for each size, and left rows keep their order.                                   |

`join()` takes an optional `JoinAlgorithmBuilder` as the last argument. One join can therefore override the configured algorithm:

```php ignore
->join($right, join_on(['id' => 'id']), JoinType::left, hash_join()->storage(new MemoryRowsStorage()))
```

`joinEach()` does not take one. It reads one extractor for each left batch, so an algorithm for each call is then
also built again for each batch.

Configure the join algorithm through `config_builder()->join(hash_join())`. All its options are on the
`hash_join()` builder. The default of `memoryLimit()` is `FLOW_MAX_MEMORY`. Without it, the limit is 70% of the
`memory_limit` of PHP, or 1 GB for `-1`. The limit applies to the full process, so a join and a sort in one pipeline
stay under it together:

```php
<?php

data_frame(
    config_builder()
        ->join(
            hash_join()
                ->memoryLimit(Unit::fromMb(512))  // process memory past which rows go to the storage
                ->bucketsCount(64)                // number of buckets
                ->batchSize(1000)                 // rows per bucket frame and per batch read back
                ->broadcastLimit(Unit::fromMb(10)) // a right side under it is broadcast instead of hashed
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
        JoinType::left_anti
    )
    ->write(to_output())
    ->run();
```

Output:

```console
+----+-----------+
| id | sku |
+----+-----------+
| 1 | PRODUCT01 |
+----+-----------+
1 rows
```

