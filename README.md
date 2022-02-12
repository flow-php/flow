# ETL Adapter: Doctrine 

![PHP Version](https://img.shields.io/packagist/php-v/flow-php/etl-adapter-doctrine)
![Tests](https://github.com/flow-php/etl-adapter-doctrine/workflows/Tests/badge.svg?branch=1.x)

## Description

Adapter for [ETL](https://github.com/flow-php/etl) using bulk operations from [Doctrine Dbal Bulk](https://github.com/flow-php/doctrine-dbal-bulk).

## Loader - DbalBulk

```php
ETL::extract(
    ...
)->transform(
    ...
)->load(
    DbalLoader('your-table-name', $bulkSize = 100, ['url' => \getenv('PGSQL_DATABASE_URL')], ['skip_conflicts' => true])
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

## Local test environment with docker

To execute integration tests for PostgreSQL you will need [docker-compose](https://docs.docker.com/compose/install/).
For a configuration, you can use prepared [docker compose file](docker-compose.yml.dist) to create your own
`docker-compose.yml`. If you don't use port 5432, then the default configuration should work for you. If in your local
environment the port is not available to use, then you can change it to a different one:

```yaml
services:
    postgres:
        image: postgres:11.3-alpine
        container_name: flow-test-db
        ports:
            - YOUR_PORT:5432
        environment:
            - POSTGRES_USER=postgres
            - POSTGRES_PASSWORD=postgres
            - POSTGRES_DB=postgres
```

Also, you need set this new port for PostgreSQL in PHPUnit configuration ([phpunit.xml](phpunit.xml.dist)):

```xml
<php>
    <env name="PGSQL_DATABASE_URL" value="postgresql://postgres:postgres@127.0.0.1:YOUR_PORT/postgres?serverVersion=11%26charset=utf8" />
</php>
```

To start the docker container, just run: `docker-compose up`. Now, you are ready to execute the entire test suite:

```bash
composer test
```

For the code coverage, please install [pcov](https://pecl.php.net/package/pcov).
