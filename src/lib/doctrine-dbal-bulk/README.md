# Doctrine Bulk

Flow PHP's Doctrine DBAL Bulk is a specialized library crafted for optimizing bulk operations in your data workflows.
This library is a prime choice for handling bulk data tasks with the Doctrine Database Abstraction Layer (DBAL),
augmenting the performance and efficiency of data insertion and manipulation tasks. The Doctrine DBAL Bulk library
encapsulates the complexities of bulk operations, presenting a streamlined API that is both powerful and easy to use. By
leveraging this library, developers can effortlessly manage bulk data processes, ensuring high throughput and
reliability even in demanding data-intensive environments. This aligns perfectly with Flow PHP's ethos of robust data
processing, making the Doctrine DBAL Bulk library an invaluable addition to your data transformation and processing
toolkit.

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
* MySQL / MariaDB
* SQLite

### Adding new Dialects

[Dialect](src/Flow/Doctrine/Bulk/Dialect/Dialect.php) is basic abstraction of this library.  
The main role of Dialect is to prepare SQL insert/update statement based
on [BulkData](src/Flow/Doctrine/Bulk/BulkData.php)
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
