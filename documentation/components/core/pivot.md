# Pivot

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

Pivot operations transform data from a long format to a wide format by rotating column values into column headers. This
is commonly used for creating cross-tabular reports and summary tables.

## Basic Pivot Operation

Pivot is part of the grouping specification: it is declared between `groupBy()` and `aggregate()` and requires
exactly one aggregation function.

**The pivot columns are declared, not discovered.** `pivot()` takes the values it will turn into columns, so the
plan can name them before a row is read. Pass them with `pivot_values(...)`, or let
`discover_pivot_values()` read the column once at build time and turn what it finds into the same declared list.

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
    ->groupBy(['region', 'product'])
    ->pivot(col('product'), pivot_values('Laptop', 'Phone')) // creates the 'Laptop' and 'Phone' columns
    ->aggregate(sum(col('sales'))->as('total_sales'))
    ->write(to_output())
    ->run();
```

**Result Structure:**

```
| region | Laptop | Phone |
|--------|--------|-------|
| North  | 2200   | 1700  |
| South  | 2400   | 1550  |
```

## Monthly Sales Pivot

Create a pivot table showing sales by month:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, discover_pivot_values, sum, avg, to_output};

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
    ->aggregate(avg(col('sales'))->as('avg_sales'))
    ->write(to_output())
    ->run();
```

**Result:**

```
| region | Jan  | Feb  | Mar  |
|--------|------|------|------|
| North  | 5000 | 5500 | 6000 |
| South  | 4500 | 4800 | 5200 |
```


## Declaring versus discovering the values

`pivot_values('Laptop', 'Phone')` is the cheap form: nothing is read to work out the columns, a value the data
never carries still produces a column of nulls, and a value the data carries but you did not declare produces no
column at all.

`discover_pivot_values()` scans the pivot column once, before the plan runs, and resolves to the declared form.
It scans **the frame as it stands at the `pivot()` call**, not the bare source - so the pivot column may be one
an earlier `withEntry()` produced, and an earlier `filter()` narrows which values exist:

```php
data_frame()
    ->read(from_array($rows))
    ->filter(col('region')->equals(lit('North')))   // only North's months are discovered
    ->groupBy(['region'])
    ->pivot(col('month'), discover_pivot_values())
    ->aggregate(sum(col('sales')));
```

The scan is `DISTINCT`, skips nulls, sorts, and is bounded - `discover_pivot_values(50)` refuses a column with
more than 50 distinct values. Because it reads the frame before the real run, it needs a source it can read
twice, and refuses one it cannot:

```
Flow\ETL\Extractor\GeneratorExtractor cannot read its dataset twice, so describing it would consume the
rows before they are extracted. Pass an array, or declare the schema with ->withSchema().
```
