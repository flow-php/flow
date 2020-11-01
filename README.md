# Doctrine Bulk 

![PHP Version](https://img.shields.io/packagist/php-v/flow-php/doctrine-dbal-bulk)
![Tests](https://github.com/flow-php/doctrine-dbal-bulk/workflows/Tests/badge.svg?branch=1.x)

## Description

It provides bulk inserts and updates for Doctrine DBAL. To use it, create a `BulkInsert` object:

```php
$bulkInsert = BulkInsert::create($dbalConnection);
$bulkInsert->insert(
    'your-table-name',
    new BulkData([
        ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
        ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
        ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
    ])
);

```

*Currently, it supports only PostgreSQL*, so if you need a different database platform, feel free to create a pull request
to the repository or create `BulkInsert` object on your own:

```php
$bulkInsert = new BulkInsert($dbalConnection, new YourQueryFactory());
```

For your implementation, you need to implement the `QueryFactory` interface. To make it easier for you, the default
implementation of that interface ([DbalQueryFactory](src/Flow/Doctrine/Bulk/QueryFactory/DbalQueryFactory.php))
is not as final, so you can extend it and make some adjustments if needed.

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
