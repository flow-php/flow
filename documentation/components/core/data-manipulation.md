# Data Manipulation

- [⬅️️ Back](/documentation/components/core/core.md)

DataFrame provides several methods for manipulating data structures and values within your datasets. These operations
allow you to add, modify, cast, and clean data efficiently.

## Type Casting with autoCast()

Automatically cast data types based on content analysis:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

data_frame()
    ->read(from_array([
        ['id' => '1', 'price' => '19.99', 'active' => 'true'],
        ['id' => '2', 'price' => '29.99', 'active' => 'false'],
        ['id' => '3', 'price' => '39.99', 'active' => 'true'],
    ]))
    ->autoCast() // Automatically cast strings to appropriate types
    ->write(to_output())
    ->run();

// Result: id becomes integer, price becomes float, active becomes boolean
```

> **Note**: `autoCast()` analyzes data patterns and attempts to convert string values to more appropriate types like
> integers, floats, booleans, and dates. Use with caution on large datasets as it requires data analysis.

## Adding Entries with withEntry()

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

### duplicateRow() - Duplicate Specific Row

```php
<?php

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product A'],
        ['id' => 2, 'name' => 'Product B'],
        ['id' => 3, 'name' => 'Product C'],
    ]))
    ->duplicateRow(1) // Duplicate the second row (0-indexed)
    ->write(to_output())
    ->run();

// Result: Row with id=2 appears twice in the output
```

## Removing Duplicates

Remove duplicate rows from your dataset:

```php
<?php

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product A', 'category' => 'Electronics'],
        ['id' => 2, 'name' => 'Product B', 'category' => 'Books'],
        ['id' => 1, 'name' => 'Product A', 'category' => 'Electronics'], // Duplicate
        ['id' => 3, 'name' => 'Product C', 'category' => 'Electronics'],
        ['id' => 2, 'name' => 'Product B', 'category' => 'Books'], // Duplicate
    ]))
    ->dropDuplicates() // Remove all duplicate rows
    ->write(to_output())
    ->run();

// Result: Only unique rows remain
```

### Selective Duplicate Removal

Remove duplicates based on specific columns:

```php
<?php

use function Flow\ETL\DSL\{data_frame, from_array, to_output};

data_frame()
    ->read(from_array([
        ['id' => 1, 'name' => 'Product A', 'version' => 1],
        ['id' => 1, 'name' => 'Product A', 'version' => 2], // Same product, different version
        ['id' => 2, 'name' => 'Product B', 'version' => 1],
        ['id' => 3, 'name' => 'Product C', 'version' => 1],
    ]))
    ->dropDuplicates('id', 'name') // Remove duplicates based on id and name only
    ->write(to_output())
    ->run();

// Result: Keep first occurrence of each id/name combination
```
