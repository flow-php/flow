# Doctrine Bulk 

# Contributing

This repo is **READ ONLY**, in order to contribute to Flow PHP project, please
open PR against [flow](https://github.com/flow-php/flow) monorepo.

Changes merged to monorepo are automatically propagated into sub repositories.

## Description

Doctrine Bulk is missing bulk upsert/insert abstraction for Doctrine DBAL.

## Installation

```
composer require flow-php/doctrine-dbal-bulk:1.x@dev
```

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