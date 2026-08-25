# Window Functions

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Window functions are a special type of function that perform calculations on a set of rows that are related to the
current row. Unlike regular aggregate functions, use of a window function does not cause rows to become grouped into a
single output row - the rows retain their separate identities. Behind the scenes, the window function is able to access
more than just the current row of the query result.

To narrow window function to a specific set of rows, you need to use `window`. Window is used to partition dataset into
smaller partitions on which later window function will be applied.

### Window Functions:

- [`ROW_NUMBER()`](/src/core/etl/src/Flow/ETL/Function/RowNumber.php)
- [`RANK()`](/src/core/etl/src/Flow/ETL/Function/Rank.php)
- [`DENSE_RANK()`](/src/core/etl/src/Flow/ETL/Function/DenseRank.php)
- [`SUM`](/src/core/etl/src/Flow/ETL/Function/Sum.php)
- [`AVERAGE`](/src/core/etl/src/Flow/ETL/Function/Average.php)
- [`COUNT`](/src/core/etl/src/Flow/ETL/Function/Count.php)

All window functions are implementing [`WindowFunction`](/src/core/etl/src/Flow/ETL/Function/WindowFunction.php)
interface.

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
    ->sortBy([ref('department'), ref('rank')])
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

## Frames

A frame narrows a window function down to a subset of the partition, relative to the current row. This is what makes
moving averages, running totals and trailing counts expressible.

```php
// every row in the department gets the same number
average(ref('salary'))->over(window()->partitionBy(ref('department')));

// 3-row moving average
average(ref('salary'))->over(
    window()
        ->partitionBy(ref('department'))
        ->orderBy(ref('date'))
        ->rowsBetween(preceding(2), current_row())
);
```

### Default frame

When no frame is defined explicitly, Flow follows the SQL default:

| Window              | Default frame                                                                            |
|---------------------|------------------------------------------------------------------------------------------|
| `orderBy()` present | all rows from the start of the partition up to and including the current row's **peers** |
| `orderBy()` absent  | the whole partition                                                                      |

Peers are rows that compare equal to the current row on **every** `ORDER BY` reference. This is why
`partitionBy()` without `orderBy()` still means "the whole partition", and why tied rows all see the same value:

```php
// date:   2024-01-01, 2024-01-01, 2024-01-02, 2024-01-03
// salary:        100,        200,        300,        400
sum(ref('salary'))->over(window()->orderBy(ref('date')));
// result:        300,        300,        600,       1000
//                 ^^^^^^^^^^^^^^ both rows tie on date, so both see both
```

To restore the pre-0.43 behaviour of aggregating over the entire partition, ask for it explicitly:

```php
sum(ref('salary'))->over(
    window()->orderBy(ref('date'))->rowsBetween(unbounded_preceding(), unbounded_following())
);
```

### Frame bounds

`rowsBetween()` takes a start and an end bound, built with these DSL functions:

| Function                 | Meaning                               |
|--------------------------|---------------------------------------|
| `unbounded_preceding()`  | the first row of the partition        |
| `preceding(int $offset)` | `$offset` rows before the current row |
| `current_row()`          | the current row                       |
| `following(int $offset)` | `$offset` rows after the current row  |
| `unbounded_following()`  | the last row of the partition         |

Bounds are clamped to the partition, so `preceding(2)` on the first row simply starts at the first row. A frame that
falls entirely outside the partition is empty - `sum()` and `average()` then return `null`
and `count()` returns `0`, matching SQL.

`rowsBetween()` counts physical rows (SQL `ROWS` mode). `RANGE` with offsets, `GROUPS` and `EXCLUDE`
are not supported yet.

A frame on a window without `orderBy()` is permitted, matching PostgreSQL and Spark.

### Ranking functions ignore frames

`row_number()`, `rank()` and `dense_rank()` always see the whole ordered partition. A frame defined alongside them is
accepted and ignored, exactly as in PostgreSQL. They do require an explicit
`orderBy()`.

`rank()` and `dense_rank()` accept multiple `orderBy()` columns - rows are peers when they match on all of them - and
follow the ordering direction, so ascending and descending windows rank in opposite directions as in PostgreSQL.

### Example

```php
data_frame()
    ->read(
        from_array([
            ['id' => 1, 'department' => 'IT', 'date' => '2024-01-01', 'salary' => 100],
            ['id' => 2, 'department' => 'IT', 'date' => '2024-01-02', 'salary' => 200],
            ['id' => 3, 'department' => 'IT', 'date' => '2024-01-03', 'salary' => 300],
            ['id' => 4, 'department' => 'IT', 'date' => '2024-01-04', 'salary' => 400],
        ])
    )
    ->withEntry(
        'moving_avg',
        average(ref('salary'))->over(
            window()
                ->partitionBy(ref('department'))
                ->orderBy(ref('date'))
                ->rowsBetween(preceding(2), current_row())
        )
    )
    ->write(to_output(false))
    ->run();
```

Output:

```console
+----+------------+------------+--------+------------+
| id | department |       date | salary | moving_avg |
+----+------------+------------+--------+------------+
|  1 |         IT | 2024-01-01 |    100 | 100.000000 |
|  2 |         IT | 2024-01-02 |    200 | 150.000000 |
|  3 |         IT | 2024-01-03 |    300 | 200.000000 |
|  4 |         IT | 2024-01-04 |    400 | 300.000000 |
+----+------------+------------+--------+------------+
4 rows
```
