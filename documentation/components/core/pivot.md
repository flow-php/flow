# Pivot

- [⬅️️ Back](/documentation/components/core/core.md)

Pivot operations transform data from a long format to a wide format by rotating column values into column headers. This
is commonly used for creating cross-tabular reports and summary tables.

## Basic Pivot Operation

Pivot can only be used after a `groupBy()` operation and requires exactly one aggregation function.

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, sum, to_output};

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
    ->groupBy('region', 'product')
    ->aggregate(sum(col('sales'))->as('total_sales'))
    ->pivot(col('product')) // Pivot by product - creates 'Laptop' and 'Phone' columns
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

use function Flow\ETL\DSL\{data_frame, from_array, col, sum, avg, to_output};

$monthlySales = data_frame()
    ->read(from_array([
        ['region' => 'North', 'month' => 'Jan', 'sales' => 5000],
        ['region' => 'North', 'month' => 'Feb', 'sales' => 5500],
        ['region' => 'North', 'month' => 'Mar', 'sales' => 6000],
        ['region' => 'South', 'month' => 'Jan', 'sales' => 4500],
        ['region' => 'South', 'month' => 'Feb', 'sales' => 4800],
        ['region' => 'South', 'month' => 'Mar', 'sales' => 5200],
    ]))
    ->groupBy('region')
    ->aggregate(avg(col('sales'))->as('avg_sales'))
    ->pivot(col('month')) // Pivot by month
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

