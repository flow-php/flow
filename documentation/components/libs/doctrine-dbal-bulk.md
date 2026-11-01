---
package: flow-php/doctrine-dbal-bulk
---

# Doctrine Bulk

[PACKAGE_NAV]

[TOC]

Doctrine DBAL Bulk is a Flow PHP library for bulk operations with the Doctrine Database Abstraction Layer (DBAL).
It handles bulk inserts, updates and deletes. The library hides the complex parts of bulk operations behind
a small API.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/doctrine-dbal-bulk.md).

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

Without types, `Bulk` reads the column types of the table from the database, once for each table and `Bulk` instance.
To bind with exactly your types, give them to `BulkData`:

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

`Bulk::delete()` removes the rows that match all columns of `BulkData`.

## Supported Dialects

- PostgreSQL
- MySQL / MariaDB
- SQLite

### Adding new Dialects

A [Dialect](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/Dialect/Dialect.php) builds the SQL of one insert,
update or delete. It uses a [BulkData](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/BulkData.php) and the
`InsertOptions` / `UpdateOptions` implementation of the dialect (`PostgreSQLInsertOptions`, `MySQLInsertOptions`,
`SqliteInsertOptions`, ...).

[DbalPlatform](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/DbalPlatform.php) picks the Dialect for a Doctrine
DBAL platform. [DbalQueryFactory](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/QueryFactory/DbalQueryFactory.php)
is the [QueryFactory](/src/lib/doctrine-dbal-bulk/src/Flow/Doctrine/Bulk/QueryFactory.php) implementation. It asks the
Dialect for the query.
