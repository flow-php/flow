![img](docs/flow_php_banner_02_2022.png)

Flow is a PHP-based, strongly typed ETL (Extract Transform Load), asynchronous data processing library with constant memory consumption.

[![Latest Stable Version](https://poser.pugx.org/flow-php/flow/v)](https://packagist.org/packages/flow-php/flow)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/flow/v/unstable)](https://packagist.org/packages/flow-php/flow)
[![License](https://poser.pugx.org/flow-php/flow/license)](https://packagist.org/packages/flow-php/flow)
[![Test Suite](https://github.com/flow-php/flow/actions/workflows/test-suite.yml/badge.svg?branch=1.x)](https://github.com/flow-php/flow/actions/workflows/test-suite.yml)

Supported PHP versions: [![PHP 8.1](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/) [![PHP 8.2](https://img.shields.io/badge/php-~8.2-8892BF.svg)](https://php.net/)

## We Stand Against Terror

<table>
  <thead>
    <tr>
      <td align="center"><a href="https://www.standwithukraine.how/" target="_blank">Stand With Ukraine</a></td>
      <td align="center"><a href="https://www.standwithus.com/">Stand With Us</a></td>
    </tr>
  </thead>
  <tbody>
    <tr>
      <td align="center"><img width="256" height="186" alt="Flag of Ukraine" src="https://upload.wikimedia.org/wikipedia/commons/thumb/4/49/Flag_of_Ukraine.svg/256px-Flag_of_Ukraine.svg.png"></td>
      <td align="center"><img width="256" height="186" alt="Flag of Israel" src="https://upload.wikimedia.org/wikipedia/commons/thumb/d/d4/Flag_of_Israel.svg/256px-Flag_of_Israel.svg.png"></td>
    </tr>
  </tbody>
</table>

> On Feb. 24, 2022, Russia declared an unprovoked war on Ukraine and launched a full-scale invasion. Russia is currently bombing peaceful Ukrainian cities, including schools and hospitals and attacking civilians who are fleeing conflict zones.

> On Oct. 7, 2023, the national holiday of Simchat Torah, Hamas terrorists initiated an attack on Israel in the early hours, targeting civilians. They unleashed violence that resulted in at least 1,400 casualties and abducted at least 200 individuals, not limited to Israelis.

--- 

## Usage
```php
<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Parquet\{from_parquet, to_parquet};
use function Flow\ETL\DSL\{data_frame, lit, ref, sum, to_output};
use Flow\ETL\Filesystem\SaveMode;

require __DIR__ . '/vendor/autoload.php';

data_frame()
    ->read(from_parquet(__FLOW_DATA__ . '/orders_flow.parquet'))
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
    ->write(to_parquet(__FLOW_OUTPUT__ . '/daily_revenue.parquet'))
    ->run();
```

```console
$ php daily_revenue.php
+------------+---------------+
| created_at | daily_revenue |
+------------+---------------+
|    2023/10 |    206,669.74 |
|    2023/09 |    227,647.47 |
|    2023/08 |    237,027.31 |
|    2023/07 |    240,111.05 |
|    2023/06 |    225,536.35 |
|    2023/05 |    234,624.74 |
|    2023/04 |    231,472.05 |
|    2023/03 |    231,697.36 |
|    2023/02 |    211,048.97 |
|    2023/01 |    225,539.81 |
+------------+---------------+
10 rows
```


## Features

* low and constant memory consumption
* reading from any data source
* writing to any data source
* rich collection of data transformation functions
* direct access to remote filesystems
* partitioning 
* grouping & aggregating
* remote file processing
* joins
* sorting
* displaying datasets as ASCII table
* validation against the schema
* window functions
* caching

📈[Project Roadmap](https://github.com/orgs/flow-php/projects/1)

## Installation 

This package is a [monorepo](https://tomasvotruba.com/blog/2019/10/28/all-you-always-wanted-to-know-about-monorepo-but-were-afraid-to-ask/).
Please check the below packages and select only those that you are going to use, 
this will reduce the number of unnecessary dependencies in your project (less maintenance).

- [ETL](src/core/etl/README.md) 
- Adapters
  - [avro](src/adapter/etl-adapter-avro/README.md)
  - [chartjs](src/adapter/etl-adapter-chartjs/README.md)
  - [csv](src/adapter/etl-adapter-csv/README.md)
  - [doctrine](src/adapter/etl-adapter-doctrine/README.md)
  - [elasticsearch](src/adapter/etl-adapter-elasticsearch/README.md)
  - [filesystem](src/adapter/etl-adapter-filesystem/README.md)
  - [google sheet](src/adapter/etl-adapter-google-sheet/README.md)
  - [http](src/adapter/etl-adapter-http/README.md)
  - [json](src/adapter/etl-adapter-json/README.md)
  - [logger](src/adapter/etl-adapter-logger/README.md)
  - [meilisearch](src/adapter/etl-adapter-meilisearch/README.md)
  - [parquet](src/adapter/etl-adapter-parquet/README.md)
  - [text](src/adapter/etl-adapter-text/README.md)
  - [xml](src/adapter/etl-adapter-xml/README.md) 
- Libraries
  - [array-dot](src/lib/array-dot/README.md)
  - [doctrine-dbal-bulk](src/lib/doctrine-dbal-bulk/README.md)
  - [dremel](src/lib/dremel/README.md)
  - [parquet](src/lib/parquet/README.md)
  - [parquet-viewer](src/lib/parquet-viewer/README.md)
  - [snappy](src/lib/snappy/README.md)

For example, if you want to work with JSON/CSV files here are the dependencies you will need to install: 

```shell
composer require flow-php/etl:^0.1 flow-php/etl-adapter-csv:^0.1 flow-php/etl-adapter-json:^0.1
```

## Docker

Since some of the Flow adapters require additional PHP extensions, we have prepared a Docker image with all the necessary dependencies.

```shell
$ docker pull ghcr.io/flow-php/flow:latest
$ docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest
Flow-PHP - Extract Transform Load - Data processing framework 0.4.0-325-g6c3e4404

Usage:
  command [options] [arguments]

Options:
  -h, --help            Display help for the given command. When no command is given display help for the list command
  -q, --quiet           Do not output any message
  -V, --version         Display this application version
      --ansi|--no-ansi  Force (or disable --no-ansi) ANSI output
  -n, --no-interaction  Do not ask any interactive question
  -v|vv|vvv, --verbose  Increase the verbosity of messages: 1 for normal output, 2 for more verbose output and 3 for debug

Available commands:
  completion             Dump the shell completion script
  help                   Display help for a command
  list                   List commands
  run                    Run ETL pipeline
 parquet
  parquet:read:data      Read data from parquet file
  parquet:read:metadata  Read metadata from parquet file
```

To simplify the usage of Flow CLI, you can create an command alias for it:

```
alias flow='docker run -v $(pwd):/flow-workspace --rm -it ghcr.io/flow-php/flow:latest'
```

Now you can use Flow CLI as follows:

```shell
flow --help
```

If you would like to try Flow, fork this repository, navigate to it through command line interface and execute following command:

```shell
$ docker run -v $(pwd):/flow-workspace --rm -it flow-php/flow:latest run /flow-workspace/examples/topics/aggregations/daily_revenue.php
```

Flow CLI will grab the pipeline definition from `examples/topics/aggregations/daily_revenue.php` file and execute it.

## Usage

In order to understand how Flow works, please read [documentation](src/core/etl/README.md)

### [Usage Examples](examples/README.md)

## Building blocks

* DataFrame - Lazy data processing frame.
* Rows - Immutable collection of `Row` objects.
* Row - Immutable, strongly typed collection of `Entry` objects.
* Entry - Immutable, strongly typed object representing a cell in a row.
* **E**xtractor (Reader) - Memory safe, Data Source returning \Generator, yielding `Rows` to the `Pipeline`
* **T**ransformer - Data transformer receiving and returning `Rows` (in most cases transformer), one instance of `Rows` at once.
* **L**oader (Writer) - Memory safe representation of Data Sink, the responsibility of Loader is to write `Rows` into destination storage, one at time.
* Pipeline - Interface representing ETL process, each received `Rows` instanced is passed through all `Pipes`, also responsible for error handling.
* Pipe - Loader of Transformer instance existing in the `Pipes` collection.

### GitHub Stars

[![Star History Chart](https://api.star-history.com/svg?repos=flow-php/flow&type=Date)](https://star-history.com/#flow-php/flow&Date)

## Sponsors

Flow PHP is sponsored by:

- [Blackfire](https://blackfire.io/) - the best PHP profiling and monitoring tool! 

