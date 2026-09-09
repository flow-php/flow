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
    ->write(to_dbal_table_insert($connection, 'users'))
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
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\{df, limit};

// Process only first 1000 rows
df()
    ->read(from_dbal_query($connection, 'SELECT * FROM large_table'))
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

The `Transformation` is expanded **once per loader instance**, on the first batch, and the nested pipeline is then
driven a single time over the whole stream. Every operation inside it answers exactly as it does on the outer frame -
`limit()` and `add_row_index()` apply across the stream, not per batch.

`to_branch($condition, $loader, $transformation)` (or `->withTransformation($transformation)`, which replaces the one
given to `to_branch()`) drives its `Transformation` the same way: the condition filters each batch first, and one
nested pipeline then spans the whole filtered stream. The memory cost, chunk
shape and failure behaviour below apply to it unchanged.

```php
use Flow\ETL\{DataFrame, Transformation};

use function Flow\ETL\DSL\{df, from_array, ref, to_output, to_transformation};

$sortById = new class implements Transformation {
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy([ref('id')]);
    }
};

df()
    ->read(from_array([/* ... */]))
    ->write(to_transformation($sortById, to_output()))
    ->run();
```

### Memory Cost

Correctness is the same everywhere; what differs between operations is how much they hold, and they hold it inside the
loader. Three groups:

| Cost                                   | Operations                                                                                                                                                      |
|----------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------|
| Grows with the whole stream            | `sortBy()`, `aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()`, `join()`, `repartition()`                                        |
| Grows with the number of distinct keys | `dropDuplicates()`, `constrain()` with a `UniqueConstraint`                                                                                                     |
| Constant                               | `select()`, `withEntry()`, `filter()`, `add_row_index()`, `limit()`, `until()`, `offset()`, `cache($id)`, `batch_size()`, `batchBy()`                           |

The first group buffers - in memory, or spilled to disk by the external sort - exactly as it does on an outer frame.
`offset()` and `cache($id)` are in the constant group: `offset()` counts the rows it skips, and `cache($id)` writes each
batch as it passes. They need the whole stream to answer correctly, not to accumulate it.

### Chunk Shape and Order

`batch_size()`, `batchBy()` and `repartition()` change only which rows are grouped into the `Rows` handed to the wrapped
loader. No row is lost or mis-assigned.

`repartition()` and `join()` also change the **order** the rows arrive in: both group their output by key rather than
emitting it in input order. `batchBy()` preserves input order and only cuts the batches at the group boundaries.

`repartition()` is not constant memory. It buckets the whole stream before it can guarantee that every row sharing a key
arrives together, so it belongs in the first group above, alongside `sortBy()` and `join()`. Writing one directory per
key is a separate thing, declared on the loader: `to_csv(...)->partitionBy('region')`.

To re-batch the pipeline itself rather than what reaches the wrapped loader, call `$df->batchSize(...)` on the frame.

### Failure Behaviour

A failure inside a `Transformation` propagates out of the loader, and no loader in that segment is closed - exactly as a
failing loader on an outer frame is never closed.

`->onError(...)` on the outer frame governs that propagation the same way it does for a plain loader. When the handler
declines to propagate, the run continues, later batches are processed through a fresh nested pipeline, and the wrapped
loader is closed. A rebuilt pipeline starts empty: anything the previous one had accumulated is gone, and stateful
operations such as `add_row_index()` restart their counters.

The handler is **not** inherited by the nested pipeline, which always propagates. To make a failure between the
transformation's own steps recoverable, set the handler inside it:

```php
$dataFrame->onError(ignore_error_handler())->with(/* ... */);
```

None of this is durability or atomicity: `closure()` both commits and closes, so a destination written up to the point
of failure can be left behind.

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