# Quick Start 

[DOC_LINK:installation.md]

[TOC]

Before you start, you need a working installation of Flow ETL. If you do not have one, go back to the [Installation](installation.md) section.

This simple example uses Flow ETL to read a CSV file, transform it and write it to a different CSV file.

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, lit, overwrite, ref, sum, to_output};

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/orders_flow.csv'))
    ->select('created_at', 'total_price', 'discount')
    ->withColumn('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withColumn('revenue', ref('total_price')->minus(ref('discount')))
    ->select('created_at', 'revenue')
    ->groupBy(['created_at'])
    ->aggregate(sum(ref('revenue')))
    ->sortBy([ref('created_at')->desc()])
    ->withColumn('daily_revenue', ref('revenue_sum')->round(lit(2))->numberFormat(lit(2)))
    ->drop('revenue_sum')
    ->write(to_output(truncate: false))
    ->withColumn('created_at', ref('created_at')->toDate('Y/m'))
    ->write(to_csv(__DIR__ . '/daily_revenue.csv')->saveMode(overwrite()))
    ->run();
```

## Data Frame 

The `data_frame()` function is the entry point to the Flow ETL DSL.

> [!TIP]
> For the best developer experience, Flow has a DSL (Domain Specific Language).
> The Flow DSL is a set of functions that build a data processing pipeline.
> The full project uses an Object-Oriented style, but the DSL is a more convenient way to build a pipeline.
> When possible, use DSL functions instead of creating objects directly.

It creates a new instance of the `Flow\ETL\Flow` class, which is the main class of the ETL.

For the full DataFrame documentation, read the [Data Frame Guide](/documentation/components/core/core.md).

[Data Frame Examples](/getting_started/#example)

## Extraction - Reading

The first step of a data processing pipeline is to read the data from a data source.
Extractors read data from a data source and convert it into a format that Flow ETL can process.
All extractors return a `\Generator` of batches (`Rows`, one column for each schema definition). Memory therefore
stays constant for each size of the source.

```php
data_frame()
    ->read(from_csv(__DIR__ . '/orders_flow.csv'));
```

In this example, the `from_csv()` function creates a new instance of the `Flow\ETL\Adapter\CSV\IO\Extractor\CSVExtractor` class.

All file-based extractors accept [glob path patterns](/documentation/components/libs/filesystem.md#glob-patterns), so you can read multiple files at the same time.

```php
data_frame()
    ->read(from_csv(__DIR__ . '/reports/*.csv'));
```  

[Extractors Examples](/reading/#example)

## Transformation

By default, extractors read all columns from the data source. To keep only the columns that you need, use the
`select()` function. You can also use the `drop()` function to drop the columns that you do not need.

```php ignore
    ->select('created_at', 'total_price', 'discount')
```

One of the main features of Flow ETL is data transformation with the `withColumn()` function.

```php ignore
    ->withColumn('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withColumn('revenue', ref('total_price')->minus(ref('discount')))
```

The `withColumn()` function accepts two arguments. The first one is the name of the new column. The second one is
the value of the new column. The value can be a literal value, a reference to an existing column or a function call.

- `ref('created_at')`: creates a reference to the `created_at` column.
- `...->cast('date')`: casts the column to a date type.
- `...->dateFormat('Y/m')`: formats the date with the `Y/m` format. created_at then becomes the string `2023/01`.

All available functions are in the [DSL](/src/core/etl/src/Flow/ETL/DSL/functions.php).

> [!TIP]
> The DSL is only a set of functions that return instances of Flow PHP objects.
> You can always create objects directly, but the DSL is a more convenient way to build a pipeline.
> All available ETL functions are in the [Functions](/src/core/etl/src/Flow/ETL/Functions) namespace.

[Transformations Examples](/transformations/#example)

## Loading - Writing

Loading, also called writing to a data source, is the last step of the data processing pipeline.
A pipeline can have more than one writer.

```php ignore
    ->write(to_output(truncate: false))
    ->write(to_csv(__DIR__ . '/daily_revenue.csv')->saveMode(overwrite()))
```

In this example, the first writer is `to_output()`. It prints the data to the console as a simple ASCII table, and
it does not truncate the output.

```php ignore
    ->write(to_csv(__DIR__ . '/daily_revenue.csv')->saveMode(overwrite()))
```

The second writer writes the data to a CSV file. `saveMode(overwrite())` sets the save mode to `overwrite`.
Four save modes are available:

- `SaveMode::Append`: if the data sink already exists, the data is appended. This can cause duplicated data, because it does not look for rows that already exist.
- `SaveMode::ExceptionIfExists`: if the data sink already exists, an error is thrown.
- `SaveMode::Ignore`: if the data sink already exists, the write is skipped.
- `SaveMode::Overwrite`: if the data sink already exists, it is removed and written again.

> [!NOTE]
> Append mode does not append to an existing file. It writes a sibling file `name-<run>.ext` next to it
> (`daily_revenue-ecd5b31525.csv`). To read all of them, use a glob: `from_csv(__DIR__ . '/daily_revenue*.csv')`.

[Loaders Examples](/writing/#example)

## Lazy Execution

Flow ETL uses lazy execution. Only a call to the `run()` function executes the pipeline.

```php ignore
    ->run();
```

Some other functions also trigger the execution, for example `fetch()`. The
[DataFrame](/src/core/etl/src/Flow/ETL/API/DataFrame.php) source code marks each function as `@lazy` or `@trigger`.
      
