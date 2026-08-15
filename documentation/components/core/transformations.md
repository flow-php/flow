# Transformations

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Introduction

Transformations are a powerful abstraction in Flow PHP that allow you to modify DataFrames in a composable and reusable
way. Unlike Transformers which operate on individual Rows, Transformations work at the DataFrame level, providing access
to the full processing pipeline.

Every Transformation implements the `Transformation` interface with a single method:

```php
interface Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame;
}
```

## Using Transformations

Transformations can be applied to DataFrames using two methods:

- `with()` - Applies one or more transformations
- `transform()` - Alias for `with()`, provides semantic clarity

Both methods accept `Transformation` objects directly or through convenient DSL functions.

```php
use function Flow\ETL\DSL\{df, from_array, select, drop};

// Using with()
df()
    ->read(from_array([/* ... */]))
    ->with(select('id', 'name'))
    ->write(to_output())
    ->run();

// Using transform() - identical behavior
df()
    ->read(from_array([/* ... */]))
    ->transform(drop('temporary_column'))
    ->write(to_output())
    ->run();
```

## Available Transformations

### Select

Select specific columns from the DataFrame, keeping only the columns you need.

```php
use function Flow\ETL\DSL\{df, from_array, select, ref};

// Select columns by name
df()
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice', 'age' => 25, 'city' => 'New York'],
        ['id' => 2, 'name' => 'Bob', 'age' => 30, 'city' => 'Los Angeles'],
    ]))
    ->with(select('id', 'name'))
    ->write(to_output())
    ->run();

// Select using References for more control
df()
    ->read(from_array([/* ... */]))
    ->with(select(ref('id'), ref('city')))
    ->write(to_output())
    ->run();
```

### Drop

Remove unwanted columns from the DataFrame, keeping all other columns.

```php
use function Flow\ETL\DSL\{df, from_array, drop, ref};

// Drop columns by name
df()
    ->read(from_array([
        ['id' => 1, 'password' => 'secret', 'name' => 'Alice'],
        ['id' => 2, 'password' => 'hidden', 'name' => 'Bob'],
    ]))
    ->with(drop('password'))
    ->write(to_output())
    ->run();

// Drop using References
df()
    ->read(from_array([/* ... */]))
    ->with(drop(ref('temp_column'), ref('debug_info')))
    ->write(to_output())
    ->run();
```

### Batch Size

Control memory usage by setting the batch size for processing. Smaller batch sizes reduce memory consumption when
processing large datasets.

```php
use function Flow\ETL\DSL\{df, from_csv, batch_size};

// Process large CSV file in batches of 100 rows
df()
    ->read(from_csv('huge_file.csv'))
    ->with(batch_size(100))
    ->write(to_database('users'))
    ->run();
```

### Add Row Index

Add an index column to each row, useful for tracking row position or creating unique identifiers.

```php
use function Flow\ETL\DSL\{df, from_array, add_row_index};
use Flow\ETL\Transformation\AddRowIndex\StartFrom;

// Add default index starting from 0
df()
    ->read(from_array([
        ['name' => 'Alice'],
        ['name' => 'Bob'],
    ]))
    ->with(add_row_index())
    ->write(to_output())
    ->run();
// Output: [['index' => 0, 'name' => 'Alice'], ['index' => 1, 'name' => 'Bob']]

// Custom column name and start from 1
df()
    ->read(from_array([/* ... */]))
    ->with(add_row_index('row_number', StartFrom::ONE))
    ->write(to_output())
    ->run();
```

### Limit

Restrict the number of rows processed, useful for debugging or sampling data.

```php
use function Flow\ETL\DSL\{df, from_database, limit};

// Process only first 1000 rows
df()
    ->read(from_database('large_table'))
    ->with(limit(1000))
    ->write(to_csv('sample.csv'))
    ->run();

// Remove limit (process all rows)
df()
    ->read(from_array([/* ... */]))
    ->with(limit(null))
    ->write(to_output())
    ->run();
```

### Mask Columns

Replace column values with a mask string, useful for hiding sensitive information.

```php
use function Flow\ETL\DSL\{df, from_array, mask_columns};

// Mask sensitive columns with default mask
df()
    ->read(from_array([
        ['name' => 'Alice', 'ssn' => '123-45-6789', 'salary' => 50000],
        ['name' => 'Bob', 'ssn' => '987-65-4321', 'salary' => 60000],
    ]))
    ->with(mask_columns(['ssn', 'salary']))
    ->write(to_output())
    ->run();
// Output: [['name' => 'Alice', 'ssn' => '******', 'salary' => '******'], ...]

// Use custom mask
df()
    ->read(from_array([/* ... */]))
    ->with(mask_columns(['credit_card'], '[REDACTED]'))
    ->write(to_output())
    ->run();
```

## Chaining Transformations

Transformations can be chained together to create complex data processing pipelines:

```php
use function Flow\ETL\DSL\{df, from_csv, select, add_row_index, limit, batch_size};

df()
    ->read(from_csv('users.csv'))
    ->with(select('id', 'name', 'email'))     // Keep only needed columns
    ->with(add_row_index('row_num'))          // Add row numbers
    ->with(limit(1000))                       // Process only first 1000
    ->with(batch_size(50))                    // Process in batches of 50
    ->write(to_json('users_sample.json'))
    ->run();
```

## Using with to_transformation Loader

The `to_transformation` loader allows you to apply transformations as part of the loading phase, enabling complex ETL
patterns:

```php
use function Flow\ETL\DSL\{df, from_array, to_transformation, to_csv, select};

// Apply transformation before loading
df()
    ->read(from_array([/* ... */]))
    ->write(
        to_transformation(
            select('id', 'name'),      // Transform data
            to_csv('output.csv')       // Then write to CSV
        )
    )
    ->run();
```

This pattern is particularly useful when you need to:

- Apply different transformations to the same data for multiple outputs
- Create transformation pipelines that can be reused
- Separate transformation logic from extraction and loading

The `Transformation` is expanded **once per loader instance**, on the first batch, and every subsequent batch is
streamed through that same pipeline. Stateful transformations - `limit()`, `add_row_index()` - therefore apply across
the whole stream, not per batch.

### Batch-Local Operations

The nested pipeline is driven once per incoming batch, and the source it reads from yields exactly that one batch.
What an operation does with that depends on what it expands to:

- a `Transformer` (`Rows -> Rows`) is built once and keeps its state across batches, so it is correct;
- a `Processor` (`Generator<Rows> -> Generator<Rows>`) buffers only the single batch it is handed, so it answers for
  that batch alone - with no error and no warning.

Of the built-in transformations only `batch_size()` and `Flow\ETL\Transformation\BatchBy` expand to a `Processor`. A
custom `Transformation` reaches the rest through the `DataFrame` methods it calls.

The following are batch-local but complete - every input row still reaches the loader, only the ordering, grouping or
batch boundaries are computed per batch instead of over the stream:

| Called inside a `Transformation` | Batch-local result |
|---|---|
| `sortBy()` | each batch is sorted on its own, the stream is not |
| `aggregate()` | one result row per batch instead of one for the whole stream |
| `groupBy()->aggregate()` | groups are never merged across batches |
| `over(window()->partitionBy(...))`, `row_number()` | the window covers one batch |
| `pivot()` | one pivoted set per batch |
| `batchBy()` | the rows and their order are correct, only the chunk boundaries are not |
| `collect()` | collects a single batch, so it does nothing |

### Operations That Lose Rows

`offset()` and `cache()` are not merely reordered or mis-grouped - rows never arrive:

- `offset()` skips its offset inside every batch separately. With batches smaller than the offset, every batch is
  consumed whole: `->offset(2)` over 3 batches of 2 rows never calls `load()` at all, where 4 rows is correct. The run
  reports success and writes an empty output.
- `cache($id)` keeps only the last batch under that id. Over the same source the run reports 6 rows and
  `df()->read(from_cache($id))` reads back 2. A partially written cache under a user-chosen id is read back later, in
  another job, with nothing to signal that it is incomplete.

### Operations That Stay Correct

Do not avoid these - `select()`, `withEntry()` with a scalar function, `map()`, `filter()`, `add_row_index()`,
`limit()`, `until()`, `dropDuplicates()` and `join()` all behave inside a `Transformation` the way they do outside it.
`join()` buffers its right side as a separate complete frame and streams the left side row by row, so it matches every
row. `limit()`, `until()` and `dropDuplicates()` return exactly what they return on the outer frame.

Window functions also go through `withEntry()` - `withEntry('avg', average(ref('v'))->over(window()))` passes a
`WindowFunction`, not a scalar function, and belongs in the batch-local table above.

### Making a Batch-Local Operation Global

Collect the outer frame before writing, so the single batch the nested pipeline receives is the whole stream:

```php
use Flow\ETL\{DataFrame, Transformation};
use function Flow\ETL\DSL\{df, from_array, ref, to_output, to_transformation};

$sortById = new class implements Transformation {
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy(ref('id'));
    }
};

df()
    ->read(from_array([/* ... */]))
    ->collect()                                  // or ->batchSize(-1)
    ->write(to_transformation($sortById, to_output()))
    ->run();
```

This holds the whole dataset in memory. Calling `->collect()` **inside** the transformation does not help, it collects
the one batch it was handed.

Running the operation on the outer frame instead is always correct and costs nothing:

```php
df()
    ->read(from_array([/* ... */]))
    ->sortBy(ref('id'))
    ->write(to_output())
    ->run();
```

To re-batch the pipeline itself, use `$df->batchSize(...)` instead of `to_transformation(batch_size(...), ...)`.

## Creating Custom Transformations

You can create custom transformations by implementing the `Transformation` interface:

```php
use Flow\ETL\{DataFrame, Transformation};

final class UppercaseNames implements Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withEntry(
            'name',
            ref('name')->upper()
        );
    }
}

// Use custom transformation
df()
    ->read(from_array([/* ... */]))
    ->with(new UppercaseNames())
    ->write(to_output())
    ->run();
```