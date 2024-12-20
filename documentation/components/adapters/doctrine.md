# ETL Adapter: Doctrine

- [⬅️️ Back](../../introduction.md)

Flow PHP's Adapter Doctrine is an adept library designed to seamlessly integrate Doctrine ORM within your ETL (Extract,
Transform, Load) workflows. This adapter is crucial for developers seeking to effortlessly interact with databases using
Doctrine ORM, ensuring a streamlined and reliable data transformation process. By harnessing the Adapter Doctrine
library, developers can tap into a robust set of features engineered for precise database interaction through Doctrine
ORM, simplifying complex data transformations and enhancing data processing efficiency. The Adapter Doctrine library
encapsulates a rich set of functionalities, offering a streamlined API for managing database tasks, which is crucial in
contemporary data processing and transformation scenarios. This library epitomizes Flow PHP's commitment to delivering
versatile and efficient data processing solutions, making it an excellent choice for developers dealing with database
operations in large-scale and data-intensive environments. With Flow PHP's Adapter Doctrine, managing database
interactions within your ETL workflows becomes a more simplified and efficient endeavor, perfectly aligning with the
robust and adaptable nature of the Flow PHP ecosystem.

## Installation

```
composer require flow-php/etl-adapter-doctrine
```

## Description

Adapter for [ETL](https://github.com/flow-php/etl) using bulk operations from [Doctrine Dbal Bulk](https://github.com/flow-php/doctrine-dbal-bulk).

## Loader - DbalLoader

```php
data_frame()
    ->read(from_())
    ->write(new DbalLoader('your-table-name', $bulkSize = 100, ['url' => \getenv('PGSQL_DATABASE_URL')], ['skip_conflicts' => true]))
    ->run();
```

All supported types of `DbalBulkLoader` loading:

- `::insert(Connection $connection, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrSkipOnConflict(Connection $connection, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrUpdateOnConstraintConflict(Connection $connection, string $table, string $constraint, QueryFactory $queryFactory = null) : self`

The `bulkSize` means how many rows you want to push to a database in a single `INSERT` query. Each extracted rows set
is going to be split before inserting data into the database.

## Extractor - DbalQuery

This simple but powerful extractor let you extract data from a single or multiple parametrized queries.

### Single Query
```php 
data_frame()
    ->read(DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} ORDER BY id"))
    ->write(to_())
    ->run()
```

### Single Parametrized Query

```php 
data_frame()
    ->read(DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} WHERE id = :id", ['id' => 1]))
    ->write(to_())
    ->run()
```
### Multiple Parametrized Query

```php 
data_frame()
    ->read(
        new DbalQueryExtractor(
            $connection
            "SELECT * FROM {$table} ORDER BY id LIMIT :limit OFFSET :offset",
            new ParametersSet(
                ['limit' => 2, 'offset' => 0],
                ['limit' => 2, 'offset' => 2],
                ['limit' => 2, 'offset' => 4],
                ['limit' => 2, 'offset' => 6],
                ['limit' => 2, 'offset' => 8],
            )
        )
    )
    ->write(to_())
    ->run()
```

In this case, query will be executed exactly five times, taking every time next entry of parameters from ParametersSet.