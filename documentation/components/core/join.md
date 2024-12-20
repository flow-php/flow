# Join

- [⬅️️ Back](core.md)

Joining two data frames is a common operation in data processing. 
It is used to combine data from two different sources into one data frame. 
The join operation is performed on a common column or columns between the two data frames.

## Join Methods

* `DataFrame::crossJoin` - join each row from the left side with each row on the right side creating `count(left) * count(right)` rows in total.
* `DataFrame::join` - right side is static for each left Rows set.
* `DataFrame::joinEach` - right side dynamically generated for each left Rows set.

## Join Types

* `left`
* `left_anti` (keep in left only what does not exist in right)
* `right`
* `inner`

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

/**
 * DataFrame::join will perform joining having both dataframes in memory.
 * This means that if if the right side dataframe is big (as the left side usually will be a batch)
 * then it might become performance bottleneck.
 * In that case please look at DataFrame::joinEach.
 */
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
| id |       sku |
+----+-----------+
|  1 | PRODUCT01 |
+----+-----------+
1 rows
```

