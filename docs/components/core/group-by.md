# Group By

- [⬅️️ Back](core.md)

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
    ->groupBy(ref('group'))
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

[➡️ Aggregations](aggregations.md)