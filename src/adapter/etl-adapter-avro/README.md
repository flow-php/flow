# ETL Adapter: Avro

## Description

ETL Adapter that provides Loaders and Extractors that works with [Apache Avro](https://avro.apache.org/) files.

Following implementation are available: 
- [Flix Tech Avro PHP](https://github.com/flix-tech/avro-php) 

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.


## Installation 

``` 
composer require flow-php/etl-adapter-avro
```

## Extractor - Flix Tech Avro

```php
<?php

(new Flow())
    ->read(Avro::from($path))
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
    ->write(Avro::to($path))
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
