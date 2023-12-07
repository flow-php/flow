# Quick Start 

- [⬅️️ Back](installation.md)

At this point, you should have a working installation of Flow ETL. If you don't, please go back to the [Installation](installation.md) section.

Let's take a look at a simple example of how to use Flow ETL to read a CSV file, transform it and write it to another CSV file.

```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\{from_csv, to_csv};
use function Flow\ETL\DSL\{data_frame, lit, ref, sum, to_output};
use Flow\ETL\Filesystem\SaveMode;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_csv(__DIR__ . '/orders_flow.csv'))
    ->select('created_at', 'total_price', 'discount')
    ->withEntry('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withEntry('revenue', ref('total_price')->minus(ref('discount')))
    ->select('created_at', 'revenue')
    ->groupBy('created_at')
    ->aggregate(sum(ref('revenue')))
    ->sortBy(ref('created_at')->desc())
    ->withEntry('daily_revenue', ref('revenue_sum')->round(lit(2))->numberFormat(lit(2)))
    ->drop('revenue_sum')
    ->write(to_output(truncate: false))
    ->withEntry('created_at', ref('created_at')->toDate('Y/m'))
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/daily_revenue.csv'))
    ->run();
```

## Data Frame 

The `data_frame()` function is the entry point to the Flow ETL DSL. 

> [!TIP]
> To maximize developer experience, Flow exposes a DSL (Domain Specific Language).
> Flow DSL is a set of functions that can be used to build a data processing pipeline.
> Entire project is written in Object-Oriented style, but DSL is a more convenient way to build a pipeline.
> Whenever possible, use DSL functions instead of creating objects directly.

It creates a new instance of the `Flow\ETL\Flow` class, which is the main class of the ETL. 

## Extraction

The first step in creating a data processing pipeline is to read the data from a data source.
Extractors are responsible for reading data from a data source and converting it into a format that can be processed by Flow ETL.
All extractors return \Generator and by design will throw rows one by one, this is to ensure that memory consumption is constant and low.

```php
data_frame()
    ->read(from_csv(__DIR__ . '/orders_flow.csv'))
```

In this example we’re using the `from_csv()` function to create a new instance of the `Flow\ETL\Adapter\CSV\CSVExtractor` class.

All file-based extractors accept [glob path patterns](https://github.com/webmozarts/glob), allowing you to read multiple files at once.

```php
data_frame()
    ->read(from_csv(__DIR__ . '/reports/*.csv'))
```  

## Transformation

Extractors by default are going to read all columns from the data source, you can use the `select()` function to select only the columns you need.
Alternatively you can use the `drop()` function to drop columns you don't need.

```php
    ->select('created_at', 'total_price', 'discount')
```

One of the most powerful features of Flow ETL is the ability to transform data using the `withEntry()` function.

```php
    ->withEntry('created_at', ref('created_at')->cast('date')->dateFormat('Y/m'))
    ->withEntry('revenue', ref('total_price')->minus(ref('discount')))
```

`withEntry()` function accepts two arguments, the first one is the name of the new entry (column), the second one is the value of the new column.
The value of the new column can be a literal value, a reference to an existing column or a function call.

- `ref('created_at')` - creates a reference to the `created_at` column.
- `...->cast('date')` - casts column to a date type.
- `...->dateFormat('Y/m')` - format date using the `Y/m` format, as a result created_at becomes a string `2023/01`.

You can find all available functions in the [DSL](../src/core/etl/src/Flow/ETL/DSL/functions.php).

> [!TIP]
> DSL is nothing more than a set of functions that return instances of Flow PHP objects. 
> You can always create objects directly, but DSL is a more convenient way to build a pipeline.
> All available ETL functions can be found in the [Function](../src/core/etl/src/Flow/ETL/Function) namespace.

## Loading

Loading, also writing to a data source, is the last step in the data processing pipeline.
There can be more than one writer in the pipeline

```php
    ->write(to_output(truncate: false))
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/daily_revenue.csv'))
```

In this example we’re first using the `to_output()` which just prints the data to the console as a simple ASCII table without
truncating the output.

```php
    ->mode(SaveMode::Overwrite)
    ->write(to_csv(__DIR__ . '/daily_revenue.csv'))
```

Second write is writing the data to a CSV file, we're using the `mode()` function to set the save mode to `overwrite`.
There are three save modes available:

- `SaveMode::Append` - If data sink already exists, data will be appended. This solution might cause data duplication since it's not check if given rows already existed.
- `SaveMode::ExceptionIfExists` - If data sink already exists error will be thrown.
- `SaveMode::Ignore` - If data sink already exists, writing will be skipped.
- `SaveMode::Overwrite` - If data sink already exists, it will be removed and written again.

> [!NOTE]
> Append mode is not really appending anything to existing files, instead it creates a folder in which it stores outputs under randomized file names. 
> It can be later read using glob-pattern, for example `from_csv('/path/to/folder/*.csv')`.

## Lazy Execution

Flow ETL is using lazy execution, which means that the pipeline will not be executed until you call the `run()` function.

```php
    ->run();
```

There are few more triggering functions, like `fetch()`, you can find which functions are `@lazy` or `@trigger` looking at
the [DataFrame](../src/core/etl/src/Flow/ETL/DataFrame.php) source code.
      
## Keep Reading
                     
To find out more about please continue reading about components and the ETL itself.

- [➡️ ETL Core](components/core/core.md)
- Adapters
  - [➡️ avro](components/adapters/avro.md)
  - [➡️ chartjs](components/adapters/chartjs.md)
  - [➡️ csv](components/adapters/csv.md)
  - [➡️ doctrine](components/adapters/doctrine.md)
  - [➡️ elasticsearch](components/adapters/elasticsearch.md)
  - [➡️ filesystem](components/adapters/filesystem.md)
  - [➡️ google-sheet](components/adapters/google-sheet.md)
  - [➡️ http](components/adapters/http.md)
  - [➡️ json](components/adapters/json.md)
  - [➡️ logger](components/adapters/logger.md)
  - [➡️ meilisearch](components/adapters/meilisearch.md)
  - [➡️ parquet](components/adapters/parquet.md)
  - [➡️ text](components/adapters/text.md)
  - [➡️ xml](components/adapters/xml.md)
- Libraries
  - [➡️ array-dot.md](components/libs/array-dot.md)
  - [➡️ doctrine-dbal-bulk.md](components/libs/doctrine-dbal-bulk.md)
  - [➡️ dremel.md](components/libs/dremel.md)
  - [➡️ parquet.md](components/libs/parquet.md)
  - [➡️ parquet-viewer.md](components/libs/parquet-viewer.md)
  - [➡️ rdsl.md](components/libs/rdsl.md)
  - [➡️ snappy.md](components/libs/snappy.md)