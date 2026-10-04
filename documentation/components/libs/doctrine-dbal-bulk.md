---
package: flow-php/doctrine-dbal-bulk
---

# Doctrine Bulk

[PACKAGE_NAV]

[TOC]

Flow PHP's Doctrine DBAL Bulk is a specialized library crafted for optimizing bulk operations in your data workflows.
This library is a prime choice for handling bulk data tasks with the Doctrine Database Abstraction Layer (DBAL),
augmenting the performance and efficiency of data insertion and manipulation tasks. The Doctrine DBAL Bulk library
encapsulates the complexities of bulk operations, presenting a streamlined API that is both powerful and easy to use. By
leveraging this library, developers can effortlessly manage bulk data processes, ensuring high throughput and
reliability even in demanding data-intensive environments. This aligns perfectly with Flow PHP's ethos of robust data
processing, making the Doctrine DBAL Bulk library an invaluable addition to your data transformation and processing
toolkit.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/doctrine-dbal-bulk.md).

## Usage Examples

Insert:

```php
<?php

use Flow\Doctrine\Bulk\{Bulk, BulkData};

Bulk::create()->insert(
    $dbalConnection,
    'users',
    new BulkData([
        ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
        ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
        ['id' => 3, 'name' => 'Name Three', 'description' => 'Description Three'],
    ]),
);
```

### Column types

Without types, `Bulk` reads the column types of the table from the database, once per table and `Bulk` instance. Pass
them to `BulkData` to bind with exactly these types:

```php
<?php

use Doctrine\DBAL\Types\{Type, Types};
use Flow\Doctrine\Bulk\{Bulk, BulkData};

Bulk::create()->insert(
    $dbalConnection,
    'users',
    new BulkData(
        [
            ['id' => 1, 'name' => 'Name One', 'created_at' => new \DateTimeImmutable()],
            ['id' => 2, 'name' => 'Name Two', 'created_at' => new \DateTimeImmutable()],
        ],
        [
            'id' => Type::getType(Types::INTEGER),
            'name' => Type::getType(Types::STRING),
            'created_at' => Type::getType(Types::DATETIME_IMMUTABLE),
        ],
    ),
);
```

Update (PostgreSQL):

```php
<?php

use Flow\Doctrine\Bulk\{Bulk, BulkData};
use Flow\Doctrine\Bulk\Dialect\PostgreSQLUpdateOptions;

Bulk::create()->update(
    $dbalConnection,
    'users',
    new BulkData([
        ['id' => 1, 'name' => 'Name One', 'description' => 'Description One'],
        ['id' => 2, 'name' => 'Name Two', 'description' => 'Description Two'],
    ]),
    new PostgreSQLUpdateOptions(primaryKeyColumns: ['id'], updateColumns: ['name']),
);
```

`Bulk::delete()` removes the rows matching every column of `BulkData`.

## Supported Dialects

- PostgreSQL
- MySQL / MariaDB
- SQLite

### Adding new Dialects

A [Dialect](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/Dialect/Dialect.php) builds the SQL of one insert,
update or delete from a [BulkData](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/BulkData.php) and the dialect's
own `InsertOptions` / `UpdateOptions` implementation (`PostgreSQLInsertOptions`, `MySQLInsertOptions`,
`SqliteInsertOptions`, ...).

[DbalPlatform](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/DbalPlatform.php) picks the Dialect for a Doctrine
DBAL platform, and [DbalQueryFactory](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/QueryFactory/DbalQueryFactory.php),
the [QueryFactory](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/QueryFactory.php) implementation, asks it for
the query.
