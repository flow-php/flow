# Fetching Data

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The client provides several methods for fetching data from PostgreSQL. All methods support parameterized queries using
positional placeholders (`$1`, `$2`, etc.).

## fetch() - First Row or Null

Returns the first row as an associative array, or `null` if no rows match:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// Simple query
$user = $client->fetch('SELECT * FROM users WHERE id = $1', [1]);

if ($user !== null) {
    echo $user['name'];
    echo $user['email'];
}

// Multiple parameters
$user = $client->fetch(
    'SELECT * FROM users WHERE email = $1 AND active = $2',
    ['john@example.com', true]
);
```

## fetchOne() - Exactly One Row

Returns exactly one row. Throws `QueryException` if zero or more than one row is found:

```php
<?php

use Flow\PostgreSql\Client\Exception\QueryException;

// When you expect exactly one result
try {
    $user = $client->fetchOne('SELECT * FROM users WHERE id = $1', [1]);
    echo $user['name'];
} catch (QueryException $e) {
    // Row not found or multiple rows returned
}

// Useful for lookups by primary key or unique constraint
$config = $client->fetchOne(
    'SELECT * FROM settings WHERE key = $1',
    ['app.timezone']
);
```

## fetchAll() - All Rows

Returns all rows as an array of associative arrays:

```php
<?php

$users = $client->fetchAll('SELECT * FROM users WHERE active = $1', [true]);

foreach ($users as $user) {
    echo $user['name'];
}

// With ordering and limit
$recentUsers = $client->fetchAll(
    'SELECT * FROM users ORDER BY created_at DESC LIMIT $1',
    [10]
);
```

> **Note**: For large result sets, consider using [cursors](/documentation/components/libs/postgresql/client-cursor.md)
> instead to avoid loading all rows into memory.

## fetchScalar() - Single Value

Returns a single value from the first column of the first row. Ideal for aggregates:

```php
<?php

// COUNT queries
$count = $client->fetchScalar('SELECT COUNT(*) FROM users WHERE active = $1', [true]);
echo "Active users: {$count}";

// MAX/MIN/AVG
$maxPrice = $client->fetchScalar('SELECT MAX(price) FROM products');

// EXISTS check
$exists = $client->fetchScalar(
    'SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)',
    ['john@example.com']
);

// Single column lookup
$name = $client->fetchScalar('SELECT name FROM users WHERE id = $1', [1]);
```

## Typed Scalar Methods

For better static analysis and type safety, use the typed scalar methods that assert the return type:

```php
<?php

// fetchScalarInt() - Returns int
$count = $client->fetchScalarInt('SELECT COUNT(*) FROM users WHERE active = $1', [true]);
// $count is guaranteed to be int

// fetchScalarBool() - Returns bool
$exists = $client->fetchScalarBool(
    'SELECT EXISTS(SELECT 1 FROM users WHERE email = $1)',
    ['john@example.com']
);
// $exists is guaranteed to be bool

// fetchScalarFloat() - Returns float
$average = $client->fetchScalarFloat('SELECT AVG(price) FROM products');
// $average is guaranteed to be float

// fetchScalarString() - Returns string
$name = $client->fetchScalarString('SELECT name FROM users WHERE id = $1', [1]);
// $name is guaranteed to be string
```

These methods throw `QueryException` if the value cannot be converted to the expected type.

## execute() - Data Modification

For INSERT, UPDATE, DELETE statements. Returns the number of affected rows:

```php
<?php

// INSERT
$affected = $client->execute(
    'INSERT INTO users (name, email) VALUES ($1, $2)',
    ['John', 'john@example.com']
);
echo "Inserted {$affected} row(s)";

// UPDATE
$affected = $client->execute(
    'UPDATE users SET active = $1 WHERE last_login < $2',
    [false, '2024-01-01']
);
echo "Deactivated {$affected} user(s)";

// DELETE
$affected = $client->execute(
    'DELETE FROM sessions WHERE expires_at < $1',
    [new \DateTimeImmutable()]
);
```

## Using Query Builder

All fetch methods accept both SQL strings and Query Builder objects:

```php
<?php

use function Flow\PostgreSql\DSL\{
    pgsql_client, pgsql_connection,
    select, col, table, eq, param, asc
};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// Build query
$query = select(col('id'), col('name'), col('email'))
    ->from(table('users'))
    ->where(eq(col('active'), param(1)))
    ->orderBy(asc(col('name')))
    ->limit(10);

// Execute with parameters
$users = $client->fetchAll($query, [true]);
```

## RETURNING Clause

Use RETURNING with fetch methods to get data back from INSERT/UPDATE/DELETE:

```php
<?php

// Get inserted row
$user = $client->fetch(
    'INSERT INTO users (name, email) VALUES ($1, $2) RETURNING *',
    ['John', 'john@example.com']
);
echo "Created user ID: {$user['id']}";

// Get updated rows
$updated = $client->fetchAll(
    'UPDATE users SET active = $1 WHERE last_login < $2 RETURNING id, email',
    [false, '2024-01-01']
);

// Get ID only
$id = $client->fetchScalar(
    'INSERT INTO users (name) VALUES ($1) RETURNING id',
    ['John']
);
```

## Parameter Types

PHP values are automatically converted to PostgreSQL types:

| PHP Type            | PostgreSQL Type                   |
|---------------------|-----------------------------------|
| `string`            | TEXT                              |
| `int`               | INTEGER/BIGINT                    |
| `float`             | DOUBLE PRECISION                  |
| `bool`              | BOOLEAN                           |
| `null`              | NULL                              |
| `DateTimeInterface` | TIMESTAMP WITH TIME ZONE          |
| `array`             | ARRAY or JSON (context-dependent) |

For explicit type control, see [Type System](/documentation/components/libs/postgresql/client-types.md).
