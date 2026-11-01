# Data Manipulation

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

DataFrame has several methods that change the data structures and values of your datasets. With these operations,
you can add, change, cast, and clean data efficiently.

## Inferring Types from an Array

An array source has no schema, so tell it to infer one:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, infer_schema};

data_frame()
    ->read(from_array([
        ['id' => '1', 'price' => '19.99', 'active' => 'true'],
        ['id' => '2', 'price' => '29.99', 'active' => 'false'],
        ['id' => '3', 'price' => '39.99', 'active' => 'true'],
    ])->inferSchema(infer_schema()))
    ->collect()
    ->printSchema();

// schema
// |-- id: ?string
// |-- price: ?string
// |-- active: ?string
```

> Note: inference reads the sample once and fixes one type for each column, so a column is never mixed. Each
> inferred column is nullable. `from_array()` infers from the PHP value, so these string cells stay strings.
> Only file sources narrow text. `from_csv()` and `from_json()` read each cell as text and narrow it to the richest
> type that parses. `'19.99'` from a CSV therefore infers as `?float`. To limit the candidates, use `->types(...)`.
> For strings only, use `->allStrings()`.

## Adding Columns with withColumn()

Add new columns or change existing columns with expressions:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, lit, concat, to_output};

data_frame()
    ->read(from_array([
        ['first_name' => 'John', 'last_name' => 'Doe', 'salary' => 50000],
        ['first_name' => 'Jane', 'last_name' => 'Smith', 'salary' => 60000],
    ]))
    ->withColumn('full_name', concat(col('first_name'), lit(' '), col('last_name')))
    ->withColumn('annual_bonus', col('salary')->multiply(lit(0.1)))
    ->write(to_output())
    ->run();
```

## Duplicating Rows

Create duplicate rows for tests or data expansion:

### duplicateRow() - Duplicate Matching Rows

`duplicateRow()` takes a boolean condition. Each row that matches it appears twice.

```php
<?php

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product A'],
        ['id' => 2, 'name' => 'Product B'],
        ['id' => 3, 'name' => 'Product C'],
    ]))
    ->duplicateRow(ref('id')->equals(lit(2)))
    ->write(to_output())
    ->run();

// Result: Row with id=2 appears twice in the output
```

## Removing Duplicates

`dropDuplicates()` takes one or more columns. It keeps the first row of each distinct combination of those columns:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product A', 'version' => 1],
        ['id' => 1, 'name' => 'Product A', 'version' => 2],
        ['id' => 2, 'name' => 'Product B', 'version' => 1],
    ]))
    ->dropDuplicates('id', 'name')
    ->write(to_output())
    ->run();

// Result: [1, 'Product A', 1], [2, 'Product B', 1]
```
