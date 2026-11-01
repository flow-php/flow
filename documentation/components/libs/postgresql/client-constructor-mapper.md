# ConstructorMapper

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

`ConstructorMapper` maps a database row directly into a class constructor by name. It does no type coercion. Values from the driver go through as they are. Use it in these cases:

- The PHP types of the driver (`int`, `string`, `bool`, `\DateTimeImmutable`, ...) already match the parameter types of your DTO.
- Column names match constructor parameter names exactly. For names that do not match, use SQL `AS` aliases.

For coercion (for example JSONB string → array, date string → `\DateTimeImmutable`), use [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md) instead. You can chain it before `ConstructorMapper`.

## DSL

```php ignore
constructor_mapper(class-string<T> $class) : ConstructorMapper<T>
```

Returns a `RowMapper<T>` ready for `fetchInto` / `fetchOneInto` / `fetchAllInto`.

## Basic Usage

```php
<?php

use function Flow\PostgreSql\DSL\{constructor_mapper, pgsql_client, pgsql_connection};

readonly class User
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
        public bool $active,
    ) {}
}

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$user = $client->fetchInto(
    constructor_mapper(User::class),
    'SELECT id, name, email, active FROM users WHERE id = $1',
    [1],
);
```

## fetchInto() - First Object or Null

Returns the first row mapped to an object, or `null` for no rows:

```php
<?php

$user = $client->fetchInto(
    constructor_mapper(User::class),
    'SELECT id, name, email, active FROM users WHERE email = $1',
    ['john@example.com'],
);
```

## fetchOneInto() - Exactly One Object

Throws `QueryException` for zero rows or more than one row:

```php
<?php

use Flow\PostgreSql\Client\Exception\QueryException;

try {
    $user = $client->fetchOneInto(
        constructor_mapper(User::class),
        'SELECT id, name, email, active FROM users WHERE id = $1',
        [1],
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
    constructor_mapper(User::class),
    'SELECT id, name, email, active FROM users WHERE active = $1 ORDER BY name',
    [true],
);

foreach ($users as $user) {
    echo $user->name;
}
```

## Column Name Aliasing

For DB columns in `snake_case` and a DTO in `camelCase`, alias the columns in the SQL:

```php
<?php

readonly class UserProfile
{
    public function __construct(
        public int $id,
        public string $firstName,
        public string $lastName,
        public \DateTimeImmutable $createdAt,
    ) {}
}

$profile = $client->fetchInto(
    constructor_mapper(UserProfile::class),
    'SELECT id,
            first_name AS "firstName",
            last_name  AS "lastName",
            created_at AS "createdAt"
       FROM users
      WHERE id = $1',
    [1],
);
```

## Nullable Parameters

Nullable constructor parameters receive `null` for a DB value of `NULL`:

```php
<?php

readonly class Product
{
    public function __construct(
        public int $id,
        public string $name,
        public float $price,
        public ?string $description,
    ) {}
}

$products = $client->fetchAllInto(
    constructor_mapper(Product::class),
    'SELECT id, name, price, description FROM products',
);
```

## Default Values

If a column is missing from the row and the constructor parameter has a default, the default is used:

```php
<?php

readonly class FeatureFlag
{
    public function __construct(
        public string $name,
        public bool $enabled = false,
    ) {}
}

$flag = $client->fetchInto(
    constructor_mapper(FeatureFlag::class),
    'SELECT name FROM flags WHERE name = $1',
    ['beta_ui'],
);
```

## Extra Columns

Extra columns in the row that do not match constructor parameters are silently ignored. You can `SELECT *` and use only what you declare:

```php
<?php

readonly class UserSummary
{
    public function __construct(
        public int $id,
        public string $name,
    ) {}
}

$summary = $client->fetchInto(
    constructor_mapper(UserSummary::class),
    'SELECT * FROM users WHERE id = $1',
    [1],
);
```

## Streaming via Cursor

```php
<?php

function processUser(object $user): void { /* your code */ }

$cursor = $client->cursor('SELECT id, name, email, active FROM large_users');

foreach ($cursor->map(constructor_mapper(User::class)) as $user) {
    processUser($user);
}
```

For more information, read [Cursors](/documentation/components/libs/postgresql/client-cursor.md).

## Custom RowMapper

If the 1:1 column-to-parameter rule of `ConstructorMapper` is not sufficient, implement `RowMapper` directly:

```php
<?php

use Flow\PostgreSql\Client\RowMapper;

readonly class UserMapper implements RowMapper
{
    /**
     * @param array<string, mixed> $row
     */
    public function map(array $row) : User
    {
        return new User(
            id: (int) $row['id'],
            name: $row['name'],
            email: $row['email'],
            active: $row['active'] === 't',
        );
    }
}

$user = $client->fetchInto(new UserMapper(), 'SELECT * FROM users WHERE id = $1', [1]);
```

## Error Handling

`ConstructorMapper` throws `Flow\PostgreSql\Client\Exception\MappingException` in these cases:

- The target class does not exist,
- The class has no constructor,
- A required parameter is missing from the row and has no default value and is not nullable.

## Type Conversion in Mappings

The driver converts PostgreSQL types to PHP types before the mapper gets them. The mapper receives:

| PostgreSQL Type        | PHP Type             |
|------------------------|----------------------|
| INTEGER, BIGINT        | `int`                |
| REAL, DOUBLE PRECISION | `float`              |
| BOOLEAN                | `bool`               |
| TEXT, VARCHAR          | `string`             |
| TIMESTAMP, TIMESTAMPTZ | `\DateTimeImmutable` |
| DATE                   | `\DateTimeImmutable` |
| JSON, JSONB            | `string`             |
| BYTEA                  | `string` (binary)    |
| NULL                   | `null`               |

For explicit type coercion (for example JSONB → structure or string → DateTimeImmutable), read [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md). For full type system control, read [Type System](/documentation/components/libs/postgresql/client-types.md).

## When to Reach for Something Else

| Need                                              | Use                                                                                                                   |
|---------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| Coerce JSONB string into array / nested structure | [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md)                                       |
| Coerce date string into `\DateTimeImmutable`      | [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md) with `type_datetime()`                |
| Hydrate complex object graphs from JSONB          | [`TypeMapper` chained with PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md) |
