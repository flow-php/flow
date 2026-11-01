# Transformations

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Introduction

A Transformation changes a DataFrame. You can combine Transformations and use them again. A Transformer gets one
batch (`Rows`) at a time. A Transformation works on the DataFrame, so it has access to the full processing pipeline.

Every Transformation implements the `Transformation` interface. The interface has one method:

```php
interface Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame;
}
```

## Using Transformations

`with()` applies a transformation to a DataFrame. It accepts a `Transformation` object, or the result of a DSL function.

```php
use function Flow\ETL\DSL\{df, from_array, select, drop, to_output};

// Keeping columns
df()
    ->read(from_array([['id' => 1, 'name' => 'Alice', 'temporary_column' => 'x']]))
    ->with(select('id', 'name'))
    ->write(to_output())
    ->run();

// Dropping a column
df()
    ->read(from_array([['id' => 1, 'name' => 'Alice', 'temporary_column' => 'x']]))
    ->with(drop('temporary_column'))
    ->write(to_output())
    ->run();
```

## Available Transformations

### Select

Keeps only the columns that you select from the DataFrame.

```php
use function Flow\ETL\DSL\{df, from_array, select, ref, to_output};

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
    ->read(from_array([['id' => 1, 'name' => 'Alice', 'city' => 'New York']]))
    ->with(select(ref('id'), ref('city')))
    ->write(to_output())
    ->run();
```

### Drop

Removes the given columns from the DataFrame. All other columns stay.

```php
use function Flow\ETL\DSL\{df, from_array, drop, ref, to_output};

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
    ->read(from_array([['id' => 1, 'temp_column' => 'x', 'debug_info' => 'y']]))
    ->with(drop(ref('temp_column'), ref('debug_info')))
    ->write(to_output())
    ->run();
```

### Batch Size

Sets the batch size for processing. For large datasets, a smaller batch size uses less memory.

```php
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{df, batch_size};

// Process large CSV file in batches of 100 rows
df()
    ->read(from_csv('huge_file.csv'))
    ->with(batch_size(100))
    ->write(to_dbal_table_insert($connection, 'users'))
    ->run();
```

### Batch By

Keeps all rows with the same value of a column in one batch. The second argument is a minimum size. It joins small
groups into one batch.

```php
use function Flow\ETL\DSL\{df, from_array, batch_by, to_output};

df()
    ->read(from_array([
        ['order_id' => 1, 'item' => 'Widget'],
        ['order_id' => 1, 'item' => 'Gadget'],
        ['order_id' => 2, 'item' => 'Gizmo'],
    ]))
    ->with(batch_by('order_id', 2))
    ->write(to_output())
    ->run();
```

### Add Row Index

Adds an index column to each row. Use it to record the position of a row or to make unique identifiers.

```php
use function Flow\ETL\DSL\{df, from_array, add_row_index, to_output};
use Flow\ETL\Operators\Transformer\AddRowIndex\StartFrom;

// Add default index starting from 0
df()
    ->read(from_array([
        ['name' => 'Alice'],
        ['name' => 'Bob'],
    ]))
    ->with(add_row_index())
    ->write(to_output())
    ->run();
// Output: [['name' => 'Alice', 'index' => 0], ['name' => 'Bob', 'index' => 1]]

// Custom column name and start from 1
df()
    ->read(from_array([['name' => 'Alice'], ['name' => 'Bob']]))
    ->with(add_row_index('row_number', StartFrom::ONE))
    ->write(to_output())
    ->run();
```

The index starts again on every run of the frame. A custom `Transformer` that keeps state between batches can do
the same. It must implement `Flow\ETL\Operators\Transformer\Stateful`. Its `fresh()` returns a new instance in its initial state,
and every run uses that instance.

### Limit

Sets the maximum number of rows to process. Use it to debug or to take a sample of the data.

```php
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\Adapter\Doctrine\from_dbal_query;
use function Flow\ETL\DSL\{df, from_array, limit, to_output};

// Process only first 1000 rows
df()
    ->read(from_dbal_query($connection, 'SELECT * FROM large_table'))
    ->with(limit(1000))
    ->write(to_csv('sample.csv'))
    ->run();

// Remove limit (process all rows)
df()
    ->read(from_array([['id' => 1], ['id' => 2]]))
    ->with(limit(null))
    ->write(to_output())
    ->run();
```

### Mask Columns

Replaces column values with a mask string. Use it to hide sensitive information.

```php
use function Flow\ETL\DSL\{df, from_array, mask_columns, to_output};

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
    ->read(from_array([['name' => 'Alice', 'credit_card' => '4111 1111 1111 1111']]))
    ->with(mask_columns(['credit_card'], '[REDACTED]'))
    ->write(to_output())
    ->run();
```

## Chaining Transformations

You can chain Transformations into one pipeline:

```php ignore
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\JSON\to_json;
use function Flow\ETL\DSL\{df, select, add_row_index, limit, batch_size};

df()
    ->read(from_csv('users.csv'))
    ->with(select('id', 'name', 'email'))     // Keep only needed columns
    ->with(add_row_index('row_num'))          // Add row numbers
    ->with(limit(1000))                       // Process only first 1000
    ->with(batch_size(50))                    // Process in batches of 50
    ->write(to_json('users_sample.json'))
    ->run();
```

## Transformations as Branches

`to_transformation()` and `to_branch()` are branches. Each branch becomes a separate root of the plan. It gets the
rows of the node where you wrote it. Flow plans and binds it together with the rest of the frame.

```php
use function Flow\ETL\Adapter\CSV\to_csv;
use function Flow\ETL\DSL\{df, from_array, lit, ref, select, to_branch, to_transformation};

df()
    ->read(from_array([['id' => 1, 'name' => 'Alice', 'active' => true], ['id' => 2, 'name' => 'Bob', 'active' => false]]))
    ->write(to_transformation(select('id', 'name'), to_csv('names.csv')))
    ->write(to_branch(ref('active')->equals(lit(true)), to_csv('active.csv')))
    ->run();
```

A branch filters first. A `to_transformation()` inside it transforms the rows that passed the condition. Branches can
be nested. Where a branch takes a loader, it also takes another branch or a transaction:

```php
to_branch(ref('active')->equals(lit(true)), to_transformation($sortById, to_csv('active.csv')));
```

The `Transformation` runs one time for each run, over the full stream that it gets. Thus `limit()` and `add_row_index()`
apply to the full stream, not to each batch. A triggering verb (`fetch()`, `count()`, `schema()`, ...) inside it runs
the plan before it, as a terminal verb does. `onError()` inside it throws `InvalidLogicException` at `write()`. Set the
error handler on the outer frame.

```php
use Flow\ETL\API\{DataFrame, Transformation};

use function Flow\ETL\DSL\{df, from_array, ref, to_output, to_transformation};

$sortById = new class implements Transformation {
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->sortBy([ref('id')]);
    }
};

df()
    ->read(from_array([['id' => 2], ['id' => 1]]))
    ->write(to_transformation($sortById, to_output()))
    ->run();
```

### Memory Cost

All operations give the same correct result inside a branch. They differ in how much data they hold, and they hold
it inside the branch. There are three groups:

| Cost                                   | Operations                                                                                                                             |
|----------------------------------------|----------------------------------------------------------------------------------------------------------------------------------------|
| Grows with the whole stream            | `sortBy()`, `aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()`, `join()`, `repartition()`               |
| Grows with the number of distinct keys | `dropDuplicates()`, `constrain()` with a `UniqueConstraint`                                                                            |
| Constant                               | `select()`, `withColumn()`, `filter()`, `add_row_index()`, `limit()`, `until()`, `offset()`, `cache($id)`, `batch_size()`, `batchBy()` |

The first group buffers rows in memory, or spills them to disk: `sortBy()` through the external sort, and `join()` through its hash-join buckets. It does this as on an outer frame.
`offset()` and `cache($id)` are in the constant group. `offset()` counts the rows that it skips. `cache($id)` writes
each batch as it passes. They need the full stream for a correct result, but they do not keep it.

### Chunk Shape and Order

`batch_size()`, `batchBy()` and `repartition()` change only which rows go into each `Rows` that the loader of the
branch gets. No row is lost or put in the wrong group.

`repartition()` and `join()` also change the order of the rows. Both group their output by key, not in input order.
`batchBy()` keeps the input order. It only cuts the batches at the group boundaries.

`repartition()` does not use constant memory. It puts the full stream into buckets first. Only then are all rows with
the same key together. Thus it is in the first group above, with `sortBy()` and `join()`. One directory for each key
is a different thing, which you declare on the loader: `to_csv(...)->partitionBy(partition_by(ref('region')))`.

To change the batches of the pipeline, and not only the batches that the loader of the branch gets, call
`$df->batchSize(...)` on the frame.

### Failure Behaviour

A branch runs under the `->onError(...)` handler of the frame. The handler gets each failure one time:

```php
df()
    ->read(from_array([['id' => 2], ['id' => 1]]))
    ->onError(skip_rows_handler())                // a failing step inside the branch skips that batch
    ->write(to_transformation($sortById, to_csv('sorted.csv')))
    ->run();
```

A failing step inside the `Transformation` is a transformation failure. For a blocking operation, the skipped batch
is all the rows that it buffered. With the default handler, the failure goes up and no loader of the run is closed.

If the `closure()` of a loader fails, the handler does not get the failure. The exception comes out of `run()`.

This does not make the writes durable or atomic. `closure()` commits and closes, so a destination can keep the data
written before the failure. For batches that write all or nothing, put the writes in a transaction:
`to_dbal_transaction()`, `to_pgsql_transaction()`.

## Creating Custom Transformations

To make a custom transformation, implement the `Transformation` interface:

```php
use Flow\ETL\API\{DataFrame, Transformation};

use function Flow\ETL\DSL\{df, from_array, ref, to_output};

final class UppercaseNames implements Transformation
{
    public function transform(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumn(
            'name',
            ref('name')->upper()
        );
    }
}

// Use custom transformation
df()
    ->read(from_array([['name' => 'alice'], ['name' => 'bob']]))
    ->with(new UppercaseNames())
    ->write(to_output())
    ->run();
```