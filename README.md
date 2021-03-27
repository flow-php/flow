# Extract Transform Load - Abstraction

[![Minimum PHP Version](https://img.shields.io/badge/php-%3E%3D%207.4-8892BF.svg)](https://php.net/)
[![Latest Stable Version](https://poser.pugx.org/flow-php/etl/v)](https://packagist.org/packages/flow-php/etl)
[![Latest Unstable Version](https://poser.pugx.org/flow-php/etl/v/unstable)](https://packagist.org/packages/flow-php/etl)
[![License](https://poser.pugx.org/flow-php/etl/license)](https://packagist.org/packages/flow-php/etl)
![Tests](https://github.com/flow-php/etl/workflows/Tests/badge.svg?branch=1.x)

## Description

Flow PHP ETL is a simple ETL (Extract Transform Load) abstraction designed to implement Filters & Pipes architecture.

## Typical Use Cases

* Sync data from external systems (API)
* File processing
* Pushing data to external systems
* Data migrations

Using this library makes sense when we need to move data from one place to another, doing some transformations in between.

For example, let's say we must synchronize data from external API periodically, transform them into our internal
data structure, filter out things that didn't change, and load in bulk into the database.

This is a perfect scenario for ETL.

## Features

* Low memory consumption even when processing thousands of records
* Type safe Rows/Row/Entry abstractions
* Filtering
* Grouping
* Built in Rows objects comparison
* Rich collection of Entry implementations
  * [ArrayEntry](src/Flow/ETL/Row/Entry/ArrayEntry.php)
  * [BooleanEntry](src/Flow/ETL/Row/Entry/BooleanEntry.php)
  * [CollectionEntry](src/Flow/ETL/Row/Entry/CollectionEntry.php)
  * [DateEntry](src/Flow/ETL/Row/Entry/DateEntry.php)
  * [DateTimeEntry](src/Flow/ETL/Row/Entry/DateTimeEntry.php)
  * [IntegerEntry](src/Flow/ETL/Row/Entry/IntegerEntry.php)
  * [JsonEntry](src/Flow/ETL/Row/Entry/JsonEntry.php)
  * [NullEntry](src/Flow/ETL/Row/Entry/NullEntry.php)
  * [ObjectEntryEntry](src/Flow/ETL/Row/Entry/ObjectEntry.php)
  * [StringEntry](src/Flow/ETL/Row/Entry/StringEntry.php)

## Installation

```bash
composer require flow-php/etl:1.x@dev
```

## Usage

```php
<?php

use Flow\ETL\ETL;
use Flow\ETL\Extractor;
use Flow\ETL\Loader;
use Flow\ETL\Row;
use Flow\ETL\Rows;
use Flow\ETL\Transformer;

require_once __DIR__ . '/../vendor/autoload.php';

$extractor = new class implements Extractor {
    public function extract(): Generator
    {
        yield new Rows(
            Row::create(
                new Row\Entry\JsonEntry('user', ['id' => 1, 'name' => 'Norbret', 'roles' => ['DEVELOPER', 'ADMIN']])
            )
        );
    }
};

$transformer = new class implements Transformer {
    public function transform(Rows $rows): Rows
    {
        return $rows->map(function (Row $row): Row {
            $dataArray = \json_decode($row->get('user')->value(), true, 512, JSON_THROW_ON_ERROR);

            return Row::create(
                new Row\Entry\IntegerEntry('id', $dataArray['id']),
                new Row\Entry\StringEntry('name', $dataArray['name']),
                new Row\Entry\ArrayEntry('roles', $dataArray['roles'])
            );
        });
    }
};

$loader = new class implements Loader {
    public function load(Rows $rows): void
    {
        var_dump($rows->toArray());
    }
};

ETL::extract($extractor)
    ->transform($transformer)
    ->load($loader);
```

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
composer install --working-dir ./tools
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
