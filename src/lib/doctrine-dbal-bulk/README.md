# Doctrine Bulk 

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Description

Doctrine Bulk is mising bulk upsert/insert abstraction for Doctrine DBAL.

## Usage Examples

Insert:
```php
$bulk = Bulk::create();
$bulk->insert(
    $dbalConnection,
    'your-table-name',
    new BulkData([
        ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
        ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
        ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
    ])
);

```

Update:
```php
$bulk = Bulk::create();
$bulk->update(
    $dbalConnection,
    'your-table-name',
    new BulkData([
        ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
        ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
        ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
    ]),
    [
        'primary_key_columns' => ['id'],
        'update_columns' => ['name']
    ]
);

```

## Supported Dialects 

* PostgreSQL

### Adding new Dialects 

[Dialect](src/Flow/Doctrine/Bulk/Dialect/Dialect.php) is basic abstraction of this library.  
The main role of Dialect is to prepare SQL insert/update statement based on [BulkData](src/Flow/Doctrine/Bulk/BulkData.php)
and provided `options`.

* `$insertOptions`
* `$updateOptions`

Options are key => value maps without predefined structure that allows to manipulate building SQL statement. 
Each dialect should define it own structure for options in order to support db engine features, including those
that are specific for given engine. 

[QueryFactory](src/Flow/Doctrine/Bulk/QueryFactory.php) is abstraction for creating queries, there is currently only one 
implementation, DbalPlatform. QueryFactory `insertOptions` and `updateOptions` is combination of all options provided
by supported Dialects where each entry must be optional. 

example:
`dialect_option?: string`

[DbalPlatform](src/Flow/Doctrine/Bulk/DbalPlatform.php) is a factory that detects which Dialect should be used for given
Doctrine DBAL Platform. 


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
