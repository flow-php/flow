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
* Built in Rows objects comparison
* Rich collection of Row Entries 

## Row Entries

* [ArrayEntry](src/Flow/ETL/Row/Entry/ArrayEntry.php)
* [BooleanEntry](src/Flow/ETL/Row/Entry/BooleanEntry.php)
* [FloatEntry](src/Flow/ETL/Row/Entry/FloatEntry.php)
* [DateTimeEntry](src/Flow/ETL/Row/Entry/DateTimeEntry.php)
* [IntegerEntry](src/Flow/ETL/Row/Entry/IntegerEntry.php)
* [NullEntry](src/Flow/ETL/Row/Entry/NullEntry.php)
* [ObjectEntryEntry](src/Flow/ETL/Row/Entry/ObjectEntry.php)
* [StringEntry](src/Flow/ETL/Row/Entry/StringEntry.php)

## Extensions  

Extensions provides generic, not really related to any specific data source/storage transformers/loaders. 

<table style="text-align:center">
<thead>
  <tr>
    <th>Name</th>
    <th>Transformer</th>
    <th>Loader (write)</th>
  </tr>
</thead>
<tbody>
  <tr>
      <td><a href="https://github.com/flow-php/etl-transformer">Transformers</a></td>
      <td>✅</td>
      <td>🚫</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-loader">Loaders</a></td>
      <td>🚫</td>
      <td>✅</td>
  </tr>
</tbody>
</table>

## Adapters

Adapters connects ETL with existing data sources/storages and including some times custom 
data entries. 

<table style="text-align:center">
<thead>
  <tr>
    <th>Name</th>
    <th>Extractor (read)</th>
    <th>Loader (write)</th>
  </tr>
</thead>
<tbody>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-memory">Memory</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-doctrine">Doctrine - DB</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-elasticsearch">Elasticsearch</a></td>
      <td>N/A</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-csv">CSV</a></td>
      <td>✅</td>
      <td>✅</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-json">JSON</a></td>
      <td>✅</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-xml">XML</a></td>
      <td>✅</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-http">HTTP</a></td>
      <td>✅</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="#">Excel</a></td>
      <td>N/A</td>
      <td>N/A</td>
  </tr>
  <tr>
      <td><a href="https://github.com/flow-php/etl-adapter-logger">Logger</a></td>
      <td>🚫</td>
      <td>✅</td>
  </tr>
</tbody>
</table>

* ✅ - at least one implementation is available 
* 🚫 - implementation not possible
* `N/A` - not implementation available yet 

**❗ If adapter that you are looking for is not available yet, and you are willing to work on one, feel free to create one as a standalone repository.**
**Well designed and documented adapters can be pulled into `flow-php` organization that will give them maintenance and security support from the organization.** 

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
                new Row\Entry\ArrayEntry('user', ['id' => 1, 'name' => 'Norbret', 'roles' => ['DEVELOPER', 'ADMIN']])
            )
        );
    }
};

$transformer = new class implements Transformer {
    public function transform(Rows $rows): Rows
    {
        return $rows->map(function (Row $row): Row {
            $dataArray = $row->get('user')->value();

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

## Error Handling 

In case of any exception in transform/load steps, ETL process will break, in order
to change that behavior please set custom [ErrorHandler](src/Flow/ETL/ErrorHandler.php). 

Error Handler defines 3 behavior using 2 methods. 

* `ErrorHandler::throw(\Throwable $error, Rows $rows) : bool`
* `ErrorHandler::skipRows(\Throwable $error, Rows $rows) : bool`

If `throw` returns true, ETL will simply throw an error.
If `skipRows' returns true, ETL will stop processing given rows, and it will try to move to the next batch.
If both methods returns false, ETL will continue processing Rows using next transformers/loaders.

There are 3 build in ErrorHandlers (look for more in adapters):

* [IgnoreError](src/Flow/ETL/ErrorHandler/IgnoreError.php)
* [SkipRows](src/Flow/ETL/ErrorHandler/SkipRows.php)
* [ThrowError](src/Flow/ETL/ErrorHandler/ThrowError.php)

Error Handling can be set directly at ETL:

```php

ETL::extract($extractor)
    ->onError(new IgnoreError())
    ->transform($transformer)
    ->load($loader);
```

## Development

In order to install dependencies please, launch following commands:

```bash
composer install
```

## Run Tests

In order to execute full test suite, please launch following command:

```bash
composer build
```

It's recommended to use [pcov](https://pecl.php.net/package/pcov) for code coverage however you can also use
xdebug by setting `XDEBUG_MODE=coverage` env variable.
