# Window Functions

- [⬅️️ Back](core.md)

Window functions are a special type of function that perform calculations on a set of rows that are related to the current row. 
Unlike regular aggregate functions, use of a window function does not cause rows to become grouped into a single output row — the rows retain their separate identities. 
Behind the scenes, the window function is able to access more than just the current row of the query result.

To narrow window function to a specific set of rows, you need to use `window`.
Window is used to partition dataset into smaller partitions on which later window function will be applied.

### Window Functions:

- [`ROW_NUMBER()`](../../../src/core/etl/src/Flow/ETL/Function/RowNumber.php)
- [`RANK()`](../../../src/core/etl/src/Flow/ETL/Function/Rank.php)
- [`DENSE_RANK()`](../../../src/core/etl/src/Flow/ETL/Function/DenseRank.php)
- [`SUM`](../../../src/core/etl/src/Flow/ETL/Function/Sum.php)
- [`AVERAGE`](../../../src/core/etl/src/Flow/ETL/Function/Average.php)
- [`COUNT`](../../../src/core/etl/src/Flow/ETL/Function/Count.php)


All window functions are implementing [`WindowFunction`](../../../src/core/etl/src/Flow/ETL/Function/WindowFunction.php) interface.

### Example

```php
data_frame()
    ->read(
        from_array([
            ['id' => 1, 'name' => 'Greg', 'department' => 'IT', 'salary' => 6000],
            ['id' => 2, 'name' => 'Michal', 'department' => 'IT', 'salary' => 5000],
            ['id' => 3, 'name' => 'Tomas', 'department' => 'Finances', 'salary' => 11_000],
            ['id' => 4, 'name' => 'John', 'department' => 'Finances', 'salary' => 9000],
            ['id' => 5, 'name' => 'Jane', 'department' => 'Finances', 'salary' => 14_000],
            ['id' => 6, 'name' => 'Janet', 'department' => 'Finances', 'salary' => 4000],
        ])
    )
    ->withEntry('rank', dense_rank()->over(window()->partitionBy(ref('department'))->orderBy(ref('salary')->desc())))
    ->sortBy(ref('department'), ref('rank'))
    ->write(to_output(false))
    ->run();
```

Output:

```console
+----+-------+------------+--------+------+
| id |  name | department | salary | rank |
+----+-------+------------+--------+------+
|  5 |  Jane |   Finances |  14000 |    1 |
|  3 | Tomas |   Finances |  11000 |    2 |
|  4 |  John |   Finances |   9000 |    3 |
|  6 | Janet |   Finances |   4000 |    4 |
+----+-------+------------+--------+------+
4 rows
+----+--------+------------+--------+------+
| id |   name | department | salary | rank |
+----+--------+------------+--------+------+
|  1 |   Greg |         IT |   6000 |    1 |
|  2 | Michal |         IT |   5000 |    2 |
+----+--------+------------+--------+------+
2 rows
```
