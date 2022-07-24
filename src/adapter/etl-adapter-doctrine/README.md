# ETL Adapter: Doctrine 

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Installation 

```
composer require flow-php/etl-adapter-doctrine:1.x@dev
```

## Description

Adapter for [ETL](https://github.com/flow-php/etl) using bulk operations from [Doctrine Dbal Bulk](https://github.com/flow-php/doctrine-dbal-bulk).

## Loader - DbalLoader

```php
ETL::extract(
    ...
)->transform(
    ...
)->load(
    new DbalLoader('your-table-name', $bulkSize = 100, ['url' => \getenv('PGSQL_DATABASE_URL')], ['skip_conflicts' => true])
);
```

All supported types of `DbalBulkLoader` loading: 

- `::insert(Connection $connection, int $bulkChunkSize, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrSkipOnConflict(Connection $connection, int $bulkChunkSize, string $table, QueryFactory $queryFactory = null) : self`
- `::insertOrUpdateOnConstraintConflict(Connection $connection, int $bulkChunkSize, string $table, string $constraint, QueryFactory $queryFactory = null) : self`

The `bulkSize` means how many rows you want to push to a database in a single `INSERT` query. Each extracted rows set
is going to be split before inserting data into the database.


## Extractor - DbalQuery

This simple but powerful extractor let you extract data from a single or multiple parametrized queries. 

### Single Query
```php 
ETL::extract(
    DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} ORDER BY id")
)->transform(
    ...
)->load(
```

### Single Parametrized Query

```php 
ETL::extract(
    DbalQueryExtractor::singleQuery($connection, "SELECT * FROM {$table} WHERE id = :id", ['id' => 1])
)->transform(
    ...
)->load(
```
### Multiple Parametrized Query

```php 
ETL::extract(
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
)->transform(
    ...
)->load(
    ...
)
```

In this case, query will be executed exactly five times, taking every time next entry of parameters from ParametersSet. 