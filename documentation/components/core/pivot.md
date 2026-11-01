# Pivot

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Pivot operations transform data from a long format to a wide format. They turn column values into column headers.
Common uses are cross-tabular reports and summary tables.

## Basic Pivot Operation

Pivot is part of the grouping specification. It is declared between `groupBy()` and `aggregate()`, and it needs
exactly one aggregation function.

The pivot columns are declared, not discovered. `pivot()` takes the values that it turns into columns, so the
plan can name them before it reads a row. Give them with `pivot_values(...)`. You can also let
`discover_pivot_values()` read the column once at build time and turn the values that it finds into the same
declared list.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, pivot_values, sum, to_output};

$salesData = data_frame()
    ->read(from_array([
        ['region' => 'North', 'product' => 'Laptop', 'month' => 'Jan', 'sales' => 1000],
        ['region' => 'North', 'product' => 'Laptop', 'month' => 'Feb', 'sales' => 1200],
        ['region' => 'North', 'product' => 'Phone', 'month' => 'Jan', 'sales' => 800],
        ['region' => 'North', 'product' => 'Phone', 'month' => 'Feb', 'sales' => 900],
        ['region' => 'South', 'product' => 'Laptop', 'month' => 'Jan', 'sales' => 1100],
        ['region' => 'South', 'product' => 'Laptop', 'month' => 'Feb', 'sales' => 1300],
        ['region' => 'South', 'product' => 'Phone', 'month' => 'Jan', 'sales' => 700],
        ['region' => 'South', 'product' => 'Phone', 'month' => 'Feb', 'sales' => 850],
    ]))
    ->groupBy(['region'])
    ->pivot(col('product'), pivot_values('Laptop', 'Phone')) // creates the 'Laptop' and 'Phone' columns
    ->aggregate(sum(col('sales')->as('total_sales')))
    ->write(to_output(truncate: false))
    ->run();
```

```console
+--------+-------------+-------------+
| region |      Laptop |       Phone |
+--------+-------------+-------------+
|  North | 2200.000000 | 1700.000000 |
|  South | 2400.000000 | 1550.000000 |
+--------+-------------+-------------+
2 rows
```

## Monthly Sales Pivot

Create a pivot table with the sales for each month:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, discover_pivot_values, sum, average, to_output};

$monthlySales = data_frame()
    ->read(from_array([
        ['region' => 'North', 'month' => 'Jan', 'sales' => 5000],
        ['region' => 'North', 'month' => 'Feb', 'sales' => 5500],
        ['region' => 'North', 'month' => 'Mar', 'sales' => 6000],
        ['region' => 'South', 'month' => 'Jan', 'sales' => 4500],
        ['region' => 'South', 'month' => 'Feb', 'sales' => 4800],
        ['region' => 'South', 'month' => 'Mar', 'sales' => 5200],
    ]))
    ->groupBy(['region'])
    ->pivot(col('month'), discover_pivot_values()) // reads the month column once at build time
    ->aggregate(average(col('sales')->as('avg_sales')))
    ->write(to_output(truncate: false))
    ->run();
```

Discovered values are sorted, strings by bytes:

```console
+--------+-------------+-------------+-------------+
| region |         Feb |         Jan |         Mar |
+--------+-------------+-------------+-------------+
|  North | 5500.000000 | 5000.000000 | 6000.000000 |
|  South | 4800.000000 | 4500.000000 | 5200.000000 |
+--------+-------------+-------------+-------------+
2 rows
```


## Declaring versus discovering the values

`pivot_values('Laptop', 'Phone')` is the cheap form. It reads nothing to find the columns. A declared value that the
data never has still produces a column of nulls. A value in the data that you did not declare produces no column.

`discover_pivot_values()` scans the pivot column once, before the plan runs, and resolves to the declared form.
It scans the frame as it is at the `pivot()` call, not the bare source. The pivot column can therefore come from an
earlier `withColumn()`, and an earlier `filter()` limits the values that exist:

```php
data_frame()
    ->read(from_array($rows))
    ->filter(col('region')->equals(lit('North')))   // only North's months are discovered
    ->groupBy(['region'])
    ->pivot(col('month'), discover_pivot_values())
    ->aggregate(sum(col('sales')));
```

The scan is `DISTINCT`, skips nulls, and sorts like [`sortBy()`](/documentation/components/core/sort.md). It is
also bounded: `discover_pivot_values(50)` refuses a column with more than 50 distinct values. It reads the frame before
the real run, so it needs a source that it can read twice. It refuses a source that it cannot read twice:

```
The frame reads a source that cannot read its dataset twice, so discover_pivot_values() cannot scan it
before the pivot runs. Declare the values with pivot_values(...).
```
