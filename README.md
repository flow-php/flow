# ETL Adapter: Parquet

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides Loaders and Extractors that works with Parquet files.

Following implementation are available: 
- [Codename Parquet](https://github.com/codename-hub/php-parquet) 

## Installation 

``` 
composer require flow-php/etl-adapter-parquet
composer require codename/parquet
```

> Parquet library is not explicitly required, you need to make sure it is available in your composer.json file.

## Memory Consumption 

Codename Parquet even that its great library that simplified really complex problem
is not really memory optimized. 

To get better understanding of parquet file format please check [docs](https://parquet.apache.org/docs/file-format/).
Please keep in mind that Parquet files are pretty much immutable. 
You can append data to the file but in order to edit one, you need to recreate it.

### Writing

While writing to parquet file, all rows needs to be first added into memory.
Please use `rows_per_group` parameter that will put given number of rows into single parquet row group.

### Reading

Reading parquet files can be optimized by reading only specific fields. 
Parquet is a binary format that comes with a schema, it's also column based, which
is a bit different approach then for example CSV files that are rows based.
Because of that it is possible to precisely localize given columns in file and read 
them one by one. 

For even better memory utilization, make sure that parquet file you are reading 
is properly divided into row groups. 

## Extractor - Codename Parquet

```php
<?php

$path = __DIR__ . '/file.parquet'

(new Flow)
    ->read(From::rows(
        $rows = new Rows(
            ...\array_map(function (int $i) : Row {
                return Row::create(
                    Entry::integer('integer', $i),
                    Entry::float('float', 1.5),
                    Entry::string('string', 'name_' . $i),
                    Entry::boolean('boolean', true),
                    Entry::datetime('datetime', new \DateTimeImmutable()),
                    Entry::json_object('json_object', ['id' => 1, 'name' => 'test']),
                    Entry::json('json', [['id' => 1, 'name' => 'test'], ['id' => 2, 'name' => 'test']]),
                    Entry::list_of_string('list_of_strings', ['a', 'b', 'c']),
                    Entry::list_of_datetime('list_of_datetimes', [new \DateTimeImmutable(), new \DateTimeImmutable(), new \DateTimeImmutable()])
                );
            }, \range(1, 100))
        )
    ))
    ->write(Parquet::to_file($path))
    ->run();
```

## Loader - Codename Parquet

```php 
<?php

$path = __DIR__ . '/file.parquet'

$rows = (new Flow())
    ->read(Parquet::from_file($path))
    ->transform(Transform::array_unpack('row'))
    ->drop('row')
    ->fetch()
    ->run();
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
