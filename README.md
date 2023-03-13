![img](docs/flow_php_banner_02_2022.png)

Flow is a PHP based, strongly typed ETL (Extract Transform Load), asynchronous data processing library with constant memory consumption.

[![Latest Stable Version](https://poser.pugx.org/flow-php/flow/v)](https://packagist.org/packages/flow-php/flow)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/flow/v/unstable)](https://packagist.org/packages/flow-php/flow)
[![License](https://poser.pugx.org/flow-php/flow/license)](https://packagist.org/packages/flow-php/flow)
[![Test Suite](https://github.com/flow-php/flow/actions/workflows/test-suite.yml/badge.svg?branch=1.x)](https://github.com/flow-php/flow/actions/workflows/test-suite.yml)

Supported PHP versions

* [![Supported PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Installation 

This package is a [monorepo](https://tomasvotruba.com/blog/2019/10/28/all-you-always-wanted-to-know-about-monorepo-but-were-afraid-to-ask/) that should not be directly installed in your project.
Please check below packages and select only those that you are going to use: 

- [ETL](src/core/etl/README.md) 
- Adapters
  - [amphp](src/adapter/etl-adapter-amphp/README.md)
  - [avro](src/adapter/etl-adapter-avro/README.md)
  - [csv](src/adapter/etl-adapter-csv/README.md)
  - [doctrine](src/adapter/etl-adapter-doctrine/README.md)
  - [elasticsearch](src/adapter/etl-adapter-elasticsearch/README.md)
  - [google sheet](src/adapter/etl-adapter-google-sheet/README.md)
  - [http](src/adapter/etl-adapter-http/README.md)
  - [json](src/adapter/etl-adapter-json/README.md)
  - [logger](src/adapter/etl-adapter-logger/README.md)
  - [parquet](src/adapter/etl-adapter-parquet/README.md)
  - [reactphp](src/adapter/etl-adapter-reactphp/README.md)
  - [text](src/adapter/etl-adapter-text/README.md)
  - [xml](src/adapter/etl-adapter-xml/README.md) 
- Libraries
  - [array-dot](src/lib/array-dot/README.md) - auto included                  
  - [doctrine-dbal-bulk](src/lib/doctrine-dbal-bulk/README.md)

For example if you want to work with json/csv files here are dependencies you will need to install: 

```
composer require flow-php/etl:^0.1 flow-php/etl-adapter-csv:^0.1 flow-php/etl-adapter-json:^0.1
```

## Contributing 

In order to run tests locally please make sure you have [docker](https://www.docker.com/) up and running.
You also need [PHP 8.1](https://www.php.net/) and [composer](https://getcomposer.org/) to be available from your CLI.

For the code coverage, please install [pcov](https://pecl.php.net/package/pcov).

### Prepare Project:

```
cp docker-compose.yml.dist docker-compose.yml
composer install 
docker compose up -d
```

### Run Test Suite 

```
composer test
```

### Run Static Analyze 

```
composer static:analyze
```

### Test everything

This command will execute exactly the same tests as we run at Github Actions before PR can get merged.
If it passes locally, you are good to open pull request. 

```
composer build 
```

## Usage

In order to understand how Flow works, please read [documentation](src/core/etl/README.md)

### [Usage Examples](examples/README.md)

## Features

* low and constant memory consumption
* asynchronous data processing
* reading from any data source
* writing to any data source
* rich collection of data transformation functions
* direct access to remote filesystems
* partitioning 
* grouping & aggregating
* remote files processing
* joins
* sorting
* displaying datasets as ASCII table
* validation against schema
* caching

## Asynchronous Processing

* [etl-adapter-amphp](https://github.com/flow-php/etl-adapter-amphp)
* [etl-adapter-reactphp](https://github.com/flow-php/etl-adapter-reactphp)

## Building blocks

* DataFrame - Lazy data processing frame.
* Rows - Immutable collection of `Row` objects.
* Row - Immutable, strongly typed collection of `Entry` objects.
* Entry - Immutable, strongly typed object representing cell in a row.
* **E**xtractor (Reader) - Memory safe, Data Source returning \Generator, yielding `Rows` to the `Pipeline`
* **T**ransformer - Data transformer receiving and returning `Rows` (in most cases transformer), one instance of `Rows` at once.
* **L**oader (Writer) - Memory safe representation of Data Sink, responsibility of Loader is to write `Rows` into destination storage, one at time.
* Pipeline - Interface representing ETL process, each received `Rows` instanced is passed through all `Pipes`, also responsible for error handling.
* Pipe - Loader of Transformer instance existing in `Pipes` collection.

### Supported PHP versions

* 8.1 - ✅
* 8.2 - ✅

## Sponsors

Flow PHP is sponsored by:

- [Blackfire](https://blackfire.io/) - the best PHP profiling and monitoring tool! 

