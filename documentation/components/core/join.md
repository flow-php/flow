# Join

- [⬅️️ Back](/documentation/components/core/core.md)

Joining two data frames is a common operation in data processing that combines data from two different sources. Flow PHP
implements joins using a **hash join algorithm** that creates a hash table from the right DataFrame and probes it with
rows from the left DataFrame.

## Join Methods

### join()

Main join method that loads the right DataFrame into memory as a hash table for efficient lookups.

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

> Flow uses hash join implementation where hashes are stored in sorted buckets to optimize memory usage and performance.

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

