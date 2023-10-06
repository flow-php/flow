# ETL Adapter: Parquet

Flow PHP's Adapter Parquet is a sophisticated library meticulously engineered to enable seamless interaction with
Parquet data formats within your ETL (Extract, Transform, Load) workflows. This adapter is crucial for developers
looking to efficiently extract from or load data into Parquet formats, ensuring a streamlined and reliable data
transformation process. By employing the Adapter Parquet library, developers can access a robust set of features
designed for precise Parquet data handling, making complex data transformations both manageable and efficient. The
Adapter Parquet library encapsulates a comprehensive set of functionalities, providing a streamlined API for engaging
with Parquet data, which is indispensable in modern data processing and transformation environments. This library
embodies Flow PHP's commitment to providing versatile and effective data processing solutions, making it a prime choice
for developers dealing with Parquet data in large-scale and data-intensive scenarios. With Flow PHP's Adapter Parquet,
managing Parquet data within your ETL workflows becomes a more simplified and efficient task, perfectly aligning with
the robust and adaptable nature of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-parquet:1.x@dev
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
