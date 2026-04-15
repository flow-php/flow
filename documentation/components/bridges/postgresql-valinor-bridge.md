# PostgreSQL Valinor Bridge

Bridge that lets [cuyz/valinor](https://valinor.cuyz.io) act as a `RowMapper` for the [flow-php/postgresql](/documentation/components/libs/postgresql.md) client. Use it when you want Valinor's strict, type-safe object hydration to materialize PostgreSQL rows into immutable DTOs or value objects.

- [Back](/documentation/introduction.md)
- [➡️ Installation](/documentation/installation/packages/postgresql-valinor-bridge.md)
- [Packagist](https://packagist.org/packages/flow-php/postgresql-valinor-bridge)
- [GitHub](https://github.com/flow-php/postgresql-valinor-bridge)
- [API Reference](/documentation/api/bridge/postgresql/valinor)

[TOC]

## Basic Usage

### With `MapperBuilder`

```php
<?php

use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_builder_mapper;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};
use CuyZ\Valinor\MapperBuilder;

readonly class User
{
    public function __construct(
        public int $id,
        public string $name,
        public string $email,
    ) {}
}

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$user = $client->fetchInto(
    valinor_builder_mapper(new MapperBuilder(), User::class),
    'SELECT id, name, email FROM users WHERE id = $1',
    [1],
);
```

### With a Pre-built `TreeMapper`

When you want to share the same Valinor configuration across many mappers, build the `TreeMapper` once and reuse it:

```php
<?php

use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_tree_mapper;
use CuyZ\Valinor\MapperBuilder;

$treeMapper = (new MapperBuilder())
    ->allowSuperfluousKeys()
    ->mapper();

$users = $client->fetchAllInto(
    valinor_tree_mapper($treeMapper, User::class),
    'SELECT id, name, email, internal_flag FROM users',
);
```

## Combining With `TypeMapper` for Value Coercion

PostgreSQL drivers return values as PHP scalars or strings — e.g. `JSONB` arrives as a JSON-encoded string, `TIMESTAMP` as `'2026-01-01 14:30:00'`, `UUID` as a string. Valinor is strict about types, so feeding raw row arrays directly often fails.

The cleanest pattern is to chain `TypeMapper` (from flow-php/postgresql) **in front of** the Valinor mapper. `TypeMapper` casts each row column to a concrete type via [flow-php/types](/documentation/components/libs/types.md), and the resulting array is passed to Valinor for object construction.

`type_mapper(...)` accepts an optional `RowMapper $next`; when provided, the cast result is forwarded to it and its return value becomes the final output.

### Decoding `JSONB` Into Nested Objects

```php
<?php

use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_builder_mapper;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, type_mapper};
use function Flow\Types\DSL\{type_integer, type_string, type_structure};
use CuyZ\Valinor\MapperBuilder;

readonly class Address
{
    public function __construct(
        public string $street,
        public string $city,
    ) {}
}

readonly class User
{
    public function __construct(
        public int $id,
        public string $name,
        public Address $address,
    ) {}
}

$mapper = type_mapper(
    type_structure([
        'id' => type_integer(),
        'name' => type_string(),
        'address' => type_structure([
            'street' => type_string(),
            'city' => type_string(),
        ]),
    ]),
    valinor_builder_mapper(new MapperBuilder(), User::class),
);

// `address` arrives from PostgreSQL as a JSON string:
//   ['id' => 1, 'name' => 'Jane', 'address' => '{"street":"Main 1","city":"Warsaw"}']
//
// TypeMapper decodes it into ['street' => 'Main 1', 'city' => 'Warsaw'] before
// handing the row to Valinor, which then constructs Address and User.
$user = $client->fetchInto($mapper, 'SELECT id, name, address FROM users WHERE id = $1', [1]);
```

### Coercing Date Strings Into `DateTimeImmutable`

PostgreSQL returns `TIMESTAMP` values like `'2026-01-01 14:30:00'`. Valinor cannot construct `\DateTimeImmutable` from a raw string without explicit configuration, but `type_datetime()` can.

```php
<?php

use function Flow\PostgreSql\Bridge\Valinor\DSL\valinor_builder_mapper;
use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_datetime, type_integer, type_string, type_structure};
use CuyZ\Valinor\MapperBuilder;

readonly class Order
{
    public function __construct(
        public int $id,
        public string $reference,
        public \DateTimeImmutable $createdAt,
    ) {}
}

$mapper = type_mapper(
    type_structure([
        'id' => type_integer(),
        'reference' => type_string(),
        'createdAt' => type_datetime(),
    ]),
    valinor_builder_mapper(new MapperBuilder(), Order::class),
);

// Row from the driver:
//   ['id' => 42, 'reference' => 'ORD-001', 'createdAt' => '2026-01-01 14:30:00']
//
// TypeMapper turns the date string into \DateTimeImmutable; Valinor accepts it
// as-is when populating Order::$createdAt.
$order = $client->fetchInto(
    $mapper,
    'SELECT id, reference, created_at AS "createdAt" FROM orders WHERE id = $1',
    [42],
);
```

The same composition handles `UUID` (`type_uuid()`), nested `JSONB` arrays into typed lists (`type_list(type_structure([...]))`), optional fields (`type_optional(...)`), and any other Type from `flow-php/types`. See [TypeMapper](/documentation/components/libs/postgresql/client-type-mapper.md) for the full list of mapper helpers shipped with the postgresql library.

## Error Handling

Both bridge mappers catch Valinor's `CuyZ\Valinor\Mapper\MappingError` and rethrow it as `Flow\PostgreSql\Client\Exception\MappingException`, preserving the original error as the previous exception:

```php
<?php

use Flow\PostgreSql\Client\Exception\MappingException;
use CuyZ\Valinor\Mapper\MappingError;

try {
    $user = $client->fetchInto($mapper, 'SELECT ... FROM users WHERE id = $1', [1]);
} catch (MappingException $e) {
    /** @var MappingError $valinorError */
    $valinorError = $e->getPrevious();

    foreach ($valinorError->messages() as $message) {
        // Inspect Valinor's per-node messages, e.g. for logging or surfacing in an API
        echo $message->path() . ': ' . $message . PHP_EOL;
    }
}
```
