# Data Manipulation

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

DataFrame provides several methods for manipulating data structures and values within your datasets. These operations
allow you to add, modify, cast, and clean data efficiently.

## Inferring Types from an Array

An array source carries no schema, so ask it to infer one:

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

> **Note**: inference reads the sample once and freezes one type per column, so a column never comes out mixed. Every
> inferred column is nullable. `from_array()` infers from the PHP value, so these string cells stay strings.
> Narrowing text is a file-source behaviour - `from_csv()` and `from_json()` read each cell as text and narrow it to
> the richest type that parses, so `'19.99'` read from a CSV infers as `?float`. Restrict the candidates with
> `->types(...)`, or take strings only with `->allStrings()`.

## Adding Columns with withEntry()

Add new columns or modify existing ones using expressions:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, col, lit, concat, to_output};

data_frame()
    ->read(from_array([
        ['first_name' => 'John', 'last_name' => 'Doe', 'salary' => 50000],
        ['first_name' => 'Jane', 'last_name' => 'Smith', 'salary' => 60000],
    ]))
    ->withEntry('full_name', concat(col('first_name'), lit(' '), col('last_name')))
    ->withEntry('annual_bonus', col('salary')->multiply(lit(0.1)))
    ->write(to_output())
    ->run();
```

## Duplicating Rows

Create duplicate rows for testing or data expansion:

### duplicateRow() - Duplicate Matching Rows

`duplicateRow()` takes a boolean condition; every row it is true for appears twice.

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

`dropDuplicates()` takes at least one column and keeps the first row of each distinct combination of those columns:

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
