# Object Mapping

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The client can automatically map database rows to PHP objects using the `fetchInto`, `fetchOneInto`, and `fetchAllInto`
methods. This is useful for working with DTOs, entities, or value objects.

## Basic Object Mapping

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, pgsql_mapper};

// Define a DTO
readonly class User
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
        public bool $active,
    ) {}
}

// Create client with mapper
$client = pgsql_client(
    pgsql_connection('host=localhost dbname=mydb'),
    mapper: pgsql_mapper(),
);

// Fetch single object (or null)
$user = $client->fetchInto(
    User::class,
    'SELECT id, name, email, active FROM users WHERE id = $1',
    [1]
);

if ($user !== null) {
    echo $user->name;  // Typed access
}
```

## fetchInto() - First Object or Null

Returns the first row mapped to an object, or `null` if no rows:

```php
<?php

$user = $client->fetchInto(
    User::class,
    'SELECT id, name, email, active FROM users WHERE email = $1',
    ['john@example.com']
);
```

## fetchOneInto() - Exactly One Object

Throws `QueryException` if zero or more than one row:

```php
<?php

use Flow\PostgreSql\Client\Exception\QueryException;

try {
    $user = $client->fetchOneInto(
        User::class,
        'SELECT id, name, email, active FROM users WHERE id = $1',
        [1]
    );
} catch (QueryException $e) {
    // Row not found or multiple rows
}
```

## fetchAllInto() - All Objects

Returns an array of objects:

```php
<?php

/** @var User[] $users */
$users = $client->fetchAllInto(
    User::class,
    'SELECT id, name, email, active FROM users WHERE active = $1 ORDER BY name',
    [true]
);

foreach ($users as $user) {
    echo $user->name;
}
```

## ConstructorMapper

The default `pgsql_mapper()` creates a `ConstructorMapper` that:

1. Maps column names directly to constructor parameter names (1:1 matching)
2. Passes values as-is to the constructor (no type coercion)
3. Supports nullable parameters for NULL values

```php
<?php

// Column names must match parameter names exactly
readonly class Product
{
    public function __construct(
        public int $id,
        public string $name,
        public float $price,
        public ?string $description,  // Nullable for NULL values
    ) {}
}

$products = $client->fetchAllInto(
    Product::class,
    'SELECT id, name, price, description FROM products'
);
```

### Column Name Aliasing

Use SQL aliases when column names don't match parameter names:

```php
<?php

// DTO with camelCase
readonly class UserProfile
{
    public function __construct(
        public int $id,
        public string $firstName,
        public string $lastName,
        public \DateTimeImmutable $createdAt,
    ) {}
}

// Alias snake_case columns to match parameter names
$profile = $client->fetchInto(
    UserProfile::class,
    'SELECT id, first_name AS firstName, last_name AS lastName, created_at AS createdAt
     FROM users WHERE id = $1',
    [1]
);
```

## Custom RowMapper

Implement `RowMapper` for custom mapping logic:

```php
<?php

use Flow\PostgreSql\Client\RowMapper;

readonly class UserMapper implements RowMapper
{
    /**
     * @param array<string, mixed> $row
     */
    public function map(string $class, array $row): object
    {
        return new User(
            id: (int) $row['id'],
            name: $row['name'],
            email: $row['email'],
            active: $row['active'] === 't',
        );
    }
}

// Use custom mapper
$client = pgsql_client(
    pgsql_connection('host=localhost dbname=mydb'),
    mapper: new UserMapper(),
);

// Or override per-call
$user = $client->fetchInto(
    User::class,
    'SELECT * FROM users WHERE id = $1',
    [1],
    mapper: new UserMapper(),
);
```

## Cursor Object Mapping

Map objects while streaming large result sets:

```php
<?php

$cursor = $client->cursor('SELECT * FROM large_table');

foreach ($cursor->map(User::class) as $user) {
    // Process one object at a time
    processUser($user);
}
```

See [Cursors](/documentation/components/libs/postgresql/client-cursor.md) for details.

## Nested Objects

For complex object graphs, use a custom mapper:

```php
<?php

readonly class OrderWithUser
{
    public function __construct(
        public int $orderId,
        public float $total,
        public User $user,
    ) {}
}

readonly class OrderMapper implements RowMapper
{
    public function map(string $class, array $row): object
    {
        $user = new User(
            id: (int) $row['user_id'],
            name: $row['user_name'],
            email: $row['user_email'],
            active: true,
        );

        return new OrderWithUser(
            orderId: (int) $row['order_id'],
            total: (float) $row['total'],
            user: $user,
        );
    }
}

$order = $client->fetchInto(
    OrderWithUser::class,
    'SELECT o.id AS order_id, o.total, u.id AS user_id, u.name AS user_name, u.email AS user_email
     FROM orders o JOIN users u ON o.user_id = u.id
     WHERE o.id = $1',
    [1],
    mapper: new OrderMapper(),
);
```

## Type Conversion in Mappings

PostgreSQL types are converted to PHP types before being passed to the mapper. The mapper receives:

| PostgreSQL Type        | PHP Type             |
|------------------------|----------------------|
| INTEGER, BIGINT        | `int`                |
| REAL, DOUBLE PRECISION | `float`              |
| BOOLEAN                | `bool`               |
| TEXT, VARCHAR          | `string`             |
| TIMESTAMP, TIMESTAMPTZ | `\DateTimeImmutable` |
| DATE                   | `\DateTimeImmutable` |
| JSON, JSONB            | `array`              |
| BYTEA                  | `string` (binary)    |
| NULL                   | `null`               |

For explicit type control, see [Type System](/documentation/components/libs/postgresql/client-types.md).
