# ETL Adapter: Avro

[![Minimum PHP Version](https://img.shields.io/badge/php-~8.1-8892BF.svg)](https://php.net/)

## Description

ETL Adapter that provides Loaders and Extractors that works with [Apache Avro](https://avro.apache.org/) files.

Following implementation are available: 
- [Flix Tech Avro PHP](https://github.com/flix-tech/avro-php) 

## Installation 

``` 
composer require flow-php/etl-adapter-avro
composer require flix-tech/avro-php
```

## Extractor - Flix Tech Avro

```php
<?php

(new Flow())
    ->read(Avro::from_file($path))
    ->transform(Transform::array_unpack('row'))
    ->drop('row')
    ->fetch()

```

## Loader - Flix Tech Avro

```php 
<?php

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
    ->write(Avro::to_file($path))
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
