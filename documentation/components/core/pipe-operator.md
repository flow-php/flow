# Pipe Operator

[DOC_LINK:/documentation/components/core/core.md]

[TOC]

## Introduction

On PHP 8.5, every DataFrame operation also works with the pipe operator `|>`. Both forms give the same result.

```php
use function Flow\ETL\DSL\{df, from_array, lit, ref, sum, to_output};

df()
    ->read(from_array([['a' => 1, 'b' => 1, 'country' => 'PL'], ['a' => 2, 'b' => 3, 'country' => 'US']]))
    ->withColumn('total', ref('a')->plus(ref('b')))
    ->filter(ref('total')->greaterThan(lit(3)))
    ->groupBy('country')->aggregate(sum(ref('total')))
    ->write(to_output())
    ->run();
```

```php ignore
use function Flow\ETL\DSL\{aggregate, df, filter, from_array, group_by, lit, read, ref, run, sum, to_output, with_column, write};

df() |> read(from_array([['a' => 1, 'b' => 1, 'country' => 'PL'], ['a' => 2, 'b' => 3, 'country' => 'US']]))
     |> with_column('total', ref('a')->plus(ref('b')))
     |> filter(ref('total')->greaterThan(lit(3)))
     |> group_by('country')
     |> aggregate([sum(ref('total'))])
     |> write(to_output())
     |> run();
```

## Steps

`$x |> $f` calls `$f($x)`. Each DSL function below returns an invokable step that calls the DataFrame method next to it.
A step that returns a DataFrame is a `Transformation`, so it also works in `->with(...)`.

| DSL step              | Method                                         |
|-----------------------|------------------------------------------------|
| `read()`              | `Flow::read()`                                 |
| `process()`           | `Flow::process()`                              |
| `add_row_index()`     | `with(new AddRowIndex(...))`                   |
| `aggregate()`         | `aggregate()`, `GroupedDataFrame::aggregate()` |
| `aggregate_by()`      | `aggregateBy()`                                |
| `batch_by()`          | `batchBy()`                                    |
| `batch_size()`        | `batchSize()`                                  |
| `cache()`             | `cache()`                                      |
| `collect_rows()`      | `collect()`                                    |
| `collect_refs()`      | `collectRefs()`                                |
| `constrain()`         | `constrain()`                                  |
| `count_rows()`        | `count()`                                      |
| `cross_join()`        | `crossJoin()`                                  |
| `display()`           | `display()`                                    |
| `drop()`              | `drop()`                                       |
| `drop_duplicates()`   | `dropDuplicates()`                             |
| `duplicate_row()`     | `duplicateRow()`                               |
| `explain()`           | `explain()`                                    |
| `fetch()`             | `fetch()`                                      |
| `filter()`            | `filter()`                                     |
| `filters()`           | `filters()`                                    |
| `for_each()`          | `forEach()`                                    |
| `get()`               | `get()`                                        |
| `get_as_array()`      | `getAsArray()`                                 |
| `get_each_as_array()` | `getEachAsArray()`                             |
| `get_schema()`        | `schema()`                                     |
| `group_by()`          | `groupBy()`                                    |
| `join()`              | `join()`                                       |
| `join_each()`         | `joinEach()`                                   |
| `limit()`             | `limit()`                                      |
| `mask_columns()`      | `with(new MaskColumns(...))`                   |
| `match_schema()`      | `match()`                                      |
| `offset()`            | `offset()`                                     |
| `on_error()`          | `onError()`                                    |
| `pivot()`             | `GroupedDataFrame::pivot()`                    |
| `print_rows()`        | `printRows()`                                  |
| `print_schema()`      | `printSchema()`                                |
| `rename()`            | `rename()`                                     |
| `rename_each()`       | `renameEach()`                                 |
| `repartition()`       | `repartition()`                                |
| `run()`               | `run()`                                        |
| `select()`            | `select()`                                     |
| `sort_by()`           | `sortBy()`                                     |
| `until()`             | `until()`                                      |
| `void()`              | `void()`                                       |
| `with()`              | `with()`                                       |
| `with_column()`       | `withColumn()`                                 |
| `with_columns()`      | `withColumns()`                                |
| `write()`             | `write()`                                      |

The name differs from the method. `count()`, `collect()` and `schema()` are other DSL functions. `match` is a
reserved keyword. `print_rows(Rows)` is now `format_rows()`.

## Custom Transformations

A `Transformation` of your own is a pipe step too, because the interface method is `__invoke`:

```php
use Flow\ETL\API\{DataFrame, Transformation};

use function Flow\ETL\DSL\ref;

final class UppercaseNames implements Transformation
{
    public function __invoke(DataFrame $dataFrame): DataFrame
    {
        return $dataFrame->withColumn('name', ref('name')->upper());
    }
}
```

```php ignore
df() |> read(from_array([['name' => 'alice']])) |> new UppercaseNames() |> fetch();
```

## PHP 8.3 and 8.4

`|>` needs PHP 8.5. The steps work on every supported PHP version as plain calls:

```php
use function Flow\ETL\DSL\{df, fetch, filter, from_array, lit, ref};

$rows = fetch()(filter(ref('a')->greaterThan(lit(1)))(df()->read(from_array([['a' => 1], ['a' => 2]]))));
```
