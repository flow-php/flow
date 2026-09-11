# StaticFactoryMapper

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

`StaticFactoryMapper` delegates row → object construction to a **public static** factory method on the target class. Use it when:

- The target class has a `private` constructor and a named static factory (`User::fromRow(...)`),
- You want to keep row-to-object logic on the domain class itself instead of scattering it across mapper implementations,
- You need custom coercion inside the factory (e.g. `created_at` string → `\DateTimeImmutable`, JSONB string → decoded array).

The factory method **must** have this exact signature:

```php ignore
public static function fromRow(array $row) : self;
```

The method **does not** receive the mapping `Context`. If your factory needs access to the originating `Query`, executing `Client`, or user-supplied data, implement [`RowMapper`](/documentation/components/libs/postgresql.md#row-mappers) directly - see the *Custom RowMapper* section of the [ConstructorMapper documentation](/documentation/components/libs/postgresql/client-constructor-mapper.md#custom-rowmapper).

For simple 1:1 constructor-parameter mapping, use [`ConstructorMapper`](/documentation/components/libs/postgresql/client-constructor-mapper.md). For type-driven coercion via `flow-php/types`, use [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md).

## DSL

```php ignore
static_factory_mapper(class-string<T> $class, non-empty-string $method) : StaticFactoryMapper<T>
```

Returns a `RowMapper<T>` ready for `fetchInto` / `fetchOneInto` / `fetchAllInto` / `Cursor::map()`.

## Basic Usage

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, static_factory_mapper};

final readonly class User
{
    private function __construct(
        public int $id,
        public string $name,
        public string $email,
        public \DateTimeImmutable $createdAt,
    ) {
    }

    /**
     * @param array<string, mixed> $row
     */
    public static function fromRow(array $row) : self
    {
        return new self(
            id: (int) $row['id'],
            name: (string) $row['name'],
            email: (string) $row['email'],
            createdAt: new \DateTimeImmutable((string) $row['created_at']),
        );
    }
}

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$user = $client->fetchOneInto(
    static_factory_mapper(User::class, 'fromRow'),
    'SELECT id, name, email, created_at FROM users WHERE id = $1',
    [1],
);
```

## fetchInto() - First Object or Null

Returns the first row mapped via the factory, or `null` if no rows:

```php
<?php

$user = $client->fetchInto(
    static_factory_mapper(User::class, 'fromRow'),
    'SELECT id, name, email, created_at FROM users WHERE email = $1',
    ['john@example.com'],
);
```

## fetchAllInto() - All Objects

```php
<?php

/** @var User[] $users */
$users = $client->fetchAllInto(
    static_factory_mapper(User::class, 'fromRow'),
    'SELECT id, name, email, created_at FROM users ORDER BY name',
);

foreach ($users as $user) {
    echo $user->name;
}
```

## Streaming via Cursor

```php
<?php

function processUser(object $user): void { /* your code */ }

$cursor = $client->cursor('SELECT id, name, email, created_at FROM large_users');

foreach ($cursor->map(static_factory_mapper(User::class, 'fromRow')) as $user) {
    processUser($user);
}
```

See [Cursors](/documentation/components/libs/postgresql/client-cursor.md) for details.

## Error Handling

`StaticFactoryMapper` throws `Flow\PostgreSql\Client\Exception\MappingException` in these cases:

| Condition                                         | Message pattern                                       |
|---------------------------------------------------|-------------------------------------------------------|
| `$class` does not exist                           | `Failed to map row to "<class>": Class does not exist`|
| Factory method does not exist on `$class`         | `Static factory method "<class>::<method>()" does not exist` |
| Factory method exists but is not declared `static`| `Factory method "<class>::<method>()" must be declared static` |
| Factory method is declared but not `public`       | `Factory method "<class>::<method>()" must be declared public` |
| Factory method throws any `\Throwable`            | `Failed to map row to "<class>": <original message>`, with the original exception available via `MappingException::getPrevious()` |

Validation of `$class` / `$method` (existence, `static`, `public`) runs **eagerly in the constructor** - a misconfigured mapper fails fast at wiring time, not at query time. Exceptions thrown by the factory method itself surface on the matching `map()` call and are wrapped in a `MappingException` whose `getPrevious()` returns the original throwable.

## When to Reach for Something Else

| Need                                                                | Use                                                                                   |
|---------------------------------------------------------------------|---------------------------------------------------------------------------------------|
| 1:1 column-to-constructor-parameter mapping with no coercion        | [`ConstructorMapper`](/documentation/components/libs/postgresql/client-constructor-mapper.md) |
| Row validation / coercion via `flow-php/types`                      | [`TypeMapper`](/documentation/components/libs/postgresql/client-type-mapper.md)       |
| Access to `Context` (query / parameters / client / catalog) in your mapping logic | Implement [`RowMapper`](/documentation/components/libs/postgresql.md#row-mappers) directly |
| Strict object hydration of complex graphs                           | [PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md) |
