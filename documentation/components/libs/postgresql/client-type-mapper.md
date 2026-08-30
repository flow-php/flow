# TypeMapper

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

`TypeMapper` validates and coerces a database row against a `Flow\Types\Type` from [flow-php/types](/documentation/components/libs/types.md). Use it when you need:

- **Strict type validation** of incoming row data,
- **Coercion** of raw driver values (e.g. JSONB strings into arrays, date strings into `\DateTimeImmutable`, JSONB into typed structures or `Flow\Types\Value\Json`),
- **Composition** with another `RowMapper` (e.g. [`ConstructorMapper`](/documentation/components/libs/postgresql/client-constructor-mapper.md) or the [PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md)) - the cast result is forwarded to the next mapper.

Internally, `TypeMapper::map()` calls `Type::cast($row)`. `cast()` is the lenient counterpart of `assert()` - it tries to coerce the value into the requested type before validating. For `JSONB`-style columns this means a JSON string starting with `{` or `[` is automatically `json_decode`'d before the inner Type is applied.

## DSL

```php
type_mapper(Type<TType> $type, ?RowMapper<TNext> $next = null) : TypeMapper<TType, TNext>
```

- When `$next` is `null`, `map()` returns whatever `$type->cast()` produces.
- When `$next` is provided, the cast result is forwarded to `$next->map()` and `$next`'s return value becomes the mapper's output.

## Basic Usage - Validating a Row Shape

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, type_mapper};
use function Flow\Types\DSL\{type_integer, type_optional, type_string, type_structure};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

$mapper = type_mapper(type_structure([
    'id' => type_integer(),
    'name' => type_string(),
    'last_name' => type_optional(type_string()),
]));

$row = $client->fetchInto(
    $mapper,
    'SELECT id, name, last_name FROM users WHERE id = $1',
    [1],
);
// $row === ['id' => 1, 'name' => 'Alice', 'last_name' => null]
```

## Coercing JSONB Columns

PostgreSQL `JSON` and `JSONB` columns arrive as **strings** when you don't use the driver's native decoding. `TypeMapper` handles all three common targets:

### JSONB → Nested Structure

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_string, type_structure};

$mapper = type_mapper(type_structure([
    'profile' => type_structure([
        'name' => type_string(),
        'address' => type_structure([
            'city' => type_string(),
            'country' => type_string(),
        ]),
    ]),
]));

// Row from driver: ['profile' => '{"name":"Alice","address":{"city":"Warsaw","country":"PL"}}']
$row = $client->fetchInto($mapper, 'SELECT profile FROM users WHERE id = $1', [1]);
// $row === ['profile' => ['name' => 'Alice', 'address' => ['city' => 'Warsaw', 'country' => 'PL']]]
```

### JSONB → `Flow\Types\Value\Json`

When you want to pass the JSON value around as a first-class object (with `->toArray()`, `__toString()`, `JsonSerializable`):

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_json, type_structure};

$mapper = type_mapper(type_structure([
    'metadata' => type_json(),
]));

// Row from driver: ['metadata' => '{"theme":"dark","notifications":true}']
$row = $client->fetchInto($mapper, 'SELECT metadata FROM users WHERE id = $1', [1]);

$json = $row['metadata'];          // \Flow\Types\Value\Json
$json->toArray();                  // ['theme' => 'dark', 'notifications' => true]
(string) $json;                    // '{"theme":"dark","notifications":true}'
```

### JSONB → Plain String

When you want the raw JSON text - for example to forward it to an HTTP response without re-encoding:

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_string, type_structure};

$mapper = type_mapper(type_structure([
    'metadata' => type_string(),
]));

// Row from driver: ['metadata' => '{"theme":"dark"}']
$row = $client->fetchInto($mapper, 'SELECT metadata FROM users WHERE id = $1', [1]);
// $row === ['metadata' => '{"theme":"dark"}']
```

### JSONB Array → List of Structures

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_list, type_string, type_structure};

$mapper = type_mapper(type_structure([
    'addresses' => type_list(type_structure([
        'city' => type_string(),
        'country' => type_string(),
    ])),
]));

// Row from driver: ['addresses' => '[{"city":"Warsaw","country":"PL"},{"city":"Berlin","country":"DE"}]']
$row = $client->fetchInto($mapper, 'SELECT addresses FROM users WHERE id = $1', [1]);
// $row === ['addresses' => [
//     ['city' => 'Warsaw', 'country' => 'PL'],
//     ['city' => 'Berlin', 'country' => 'DE'],
// ]]
```

## Coercing Date / Time Columns

PostgreSQL `TIMESTAMP` / `TIMESTAMPTZ` already come back as `\DateTimeImmutable` from `ext-pgsql`. But text columns containing date/time strings (e.g. ISO formats stored as `TEXT`) need explicit coercion.

### String → `\DateTimeImmutable`

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_datetime, type_integer, type_structure};

$mapper = type_mapper(type_structure([
    'id' => type_integer(),
    'created_at' => type_datetime(),
]));

// Row from driver: ['id' => 42, 'created_at' => '2026-01-01 14:30:00']
$row = $client->fetchInto($mapper, 'SELECT id, created_at FROM events WHERE id = $1', [42]);

$row['created_at'];                          // \DateTimeImmutable
$row['created_at']->format('Y-m-d H:i:s');   // '2026-01-01 14:30:00'
```

`type_datetime()->cast(...)` accepts strings, numeric Unix timestamps, `\DateTime`, `\DateTimeImmutable`, and `\DateInterval`.

### String → Date

For `DATE` columns (no time component) stored as text:

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_date, type_structure};

$mapper = type_mapper(type_structure([
    'birthday' => type_date(),
]));

// Row from driver: ['birthday' => '1990-04-15']
$row = $client->fetchInto($mapper, 'SELECT birthday FROM users WHERE id = $1', [1]);

$row['birthday'];                  // \DateTimeImmutable normalized to date
$row['birthday']->format('Y-m-d'); // '1990-04-15'
```

## Coercing UUID Columns

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{type_string, type_structure, type_uuid};

$mapper = type_mapper(type_structure([
    'id' => type_uuid(),
    'name' => type_string(),
]));

// Row from driver: ['id' => '019d9293-793d-7036-918a-f19abffd545c', 'name' => 'Alice']
$row = $client->fetchInto($mapper, 'SELECT id, name FROM users WHERE id = $1', [$id]);

$row['id'];        // \Flow\Types\Value\Uuid
(string) $row['id']; // '019d9293-793d-7036-918a-f19abffd545c'
```

## Optional Fields

Absent and null are two different facts about a structure field. Declare a field as
`structure_element(..., optional: true)` when the driver row may omit the column entirely; wrap its
type in `type_optional(...)` when a present column may hold `null`:

```php
<?php

use function Flow\PostgreSql\DSL\type_mapper;
use function Flow\Types\DSL\{structure_element, type_optional, type_string, type_structure};

$mapper = type_mapper(type_structure([
    'name' => type_string(),
    'nickname' => structure_element('nickname', type_string(), optional: true),
]));

// Row: ['name' => 'Alice', 'nickname' => 'Ali']  → ['name' => 'Alice', 'nickname' => 'Ali']
// Row: ['name' => 'Alice']                       → ['name' => 'Alice']
```

## Chaining Into Another `RowMapper` (`$next`)

Pass any `RowMapper<T>` as the second argument and `TypeMapper` will forward its cast result to it. This is useful for **decode-then-hydrate** pipelines: have `TypeMapper` clean up the raw row (decode JSONB, parse dates) and then hand the clean array to a class-aware mapper.

### Chained with `ConstructorMapper`

```php
<?php

use function Flow\PostgreSql\DSL\{constructor_mapper, type_mapper};
use function Flow\Types\DSL\{type_datetime, type_integer, type_string, type_structure};

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
    constructor_mapper(Order::class),
);

// Row: ['id' => 42, 'reference' => 'ORD-001', 'createdAt' => '2026-01-01 14:30:00']
$order = $client->fetchInto(
    $mapper,
    'SELECT id, reference, created_at AS "createdAt" FROM orders WHERE id = $1',
    [42],
);
// $order instanceof Order, $order->createdAt instanceof \DateTimeImmutable
```

### Chained with the Valinor Bridge

For complex object graphs (nested DTOs, lists of value objects), combine `TypeMapper` with the [PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md) - `TypeMapper` decodes JSONB and parses dates, then Valinor builds the strict object tree.

## Error Handling

When `Type::cast()` fails (or the chained `$next` mapper throws), `TypeMapper` wraps the error in `Flow\PostgreSql\Client\Exception\MappingException` and exposes the original via `getPrevious()`:

```php
<?php

use Flow\PostgreSql\Client\Exception\MappingException;

try {
    $row = $client->fetchInto($mapper, 'SELECT ... FROM ... WHERE id = $1', [$id]);
} catch (MappingException $e) {
    $cause = $e->getPrevious(); // e.g. Flow\Types\Exception\CastingException
}
```
