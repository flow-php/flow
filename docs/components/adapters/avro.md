# Avro Adapter

- [⬅️️ Back](../../introduction.md)

Flow PHP's Adapter Avro is a finely engineered library designed to facilitate seamless interaction with Avro data
formats within your ETL (Extract, Transform, Load) workflows. This adapter is crucial for developers looking to
effortlessly extract from or load data into Avro formats, ensuring a streamlined and reliable data transformation
process. By harnessing the Adapter Avro library, developers can access a robust set of features tailored for precise
Avro data handling, simplifying complex data transformations and enhancing data processing efficiency. The Adapter Avro
library encapsulates a wide range of functionalities, providing a streamlined API for engaging with Avro data, which is
essential in modern data processing and transformation scenarios. This library exemplifies Flow PHP's dedication to
offering versatile and efficient data processing solutions, making it an optimal choice for developers dealing with Avro
data in large-scale and data-intensive projects. With Flow PHP's Adapter Avro, managing Avro data within your ETL
workflows becomes a more simplified and efficient task, perfectly aligning with the robust and adaptable nature of the
Flow PHP ecosystem.

## Installation

``` 
composer require flow-php/etl-adapter-avro:1.x@dev
```

## Extractor - Flix Tech Avro

```php
<?php

data_frame()
    ->read(from_avro($path))
    ->fetch()

```

## Loader - Flix Tech Avro

```php 
<?php

data_frame()
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
    ->write(to_avro($path))
    ->run();
```