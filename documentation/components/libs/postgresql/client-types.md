# Type System

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The client automatically converts values between PHP and PostgreSQL types. This document covers the built-in converters,
explicit type hints, and custom converter implementation.

> **Note:** The type system uses [Flow Types](/documentation/components/libs/types.md) for type hints with the `typed()`
> function.
> Type functions like `type_uuid()`, `type_json()`, etc. are imported from `Flow\Types\DSL`.

## Automatic Type Conversion

PHP values are automatically converted when binding parameters:

| PHP Type             | PostgreSQL Type           |
|----------------------|---------------------------|
| `string`             | TEXT                      |
| `int`                | INTEGER (INT4)            |
| `float`              | DOUBLE PRECISION (FLOAT8) |
| `bool`               | BOOLEAN                   |
| `null`               | NULL                      |
| `\DateTimeInterface` | TIMESTAMP WITH TIME ZONE  |
| `array`              | JSON (via json_encode)    |

PostgreSQL values are converted back to PHP when fetching:

| PostgreSQL Type        | PHP Type                |
|------------------------|-------------------------|
| TEXT, VARCHAR, CHAR    | `string`                |
| INT2, INT4, INT8       | `int`                   |
| FLOAT4, FLOAT8         | `float`                 |
| BOOLEAN                | `bool`                  |
| TIMESTAMP, TIMESTAMPTZ | `\DateTimeImmutable`    |
| DATE                   | `\DateTimeImmutable`    |
| TIME, TIMETZ           | `\DateTimeImmutable`    |
| JSON, JSONB            | `Flow\Types\Value\Json` |
| UUID                   | `Flow\Types\Value\Uuid` |
| BYTEA                  | `string` (binary)       |
| INTERVAL               | `\DateInterval`         |
| Arrays                 | `array`                 |

## Explicit Type Hints with typed()

When automatic detection isn't sufficient, use `typed()` to specify the exact type:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, typed};
use function Flow\Types\DSL\{type_uuid, type_json, type_date};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// UUID - string that should be treated as UUID
$client->fetch(
    'SELECT * FROM users WHERE id = $1',
    [typed('550e8400-e29b-41d4-a716-446655440000', type_uuid())]
);

// JSON - array or object to be sent as JSON
$client->execute(
    'INSERT INTO events (payload) VALUES ($1)',
    [typed(['event' => 'login', 'user_id' => 42], type_json())]
);

// DATE - DateTime that should be DATE, not TIMESTAMP
$client->fetch(
    'SELECT * FROM events WHERE date = $1',
    [typed(new \DateTimeImmutable('2024-01-15'), type_date())]
);
```

### Common Type Functions

These functions are from `Flow\Types\DSL`. See [Types documentation](/documentation/components/libs/types.md) for full
reference.

| Function          | PostgreSQL Type | Use Case                              |
|-------------------|-----------------|---------------------------------------|
| `type_uuid()`     | UUID            | String UUIDs                          |
| `type_json()`     | JSON            | Objects/arrays as JSON                |
| `type_date()`     | DATE            | Dates without time                    |
| `type_datetime()` | TIMESTAMP       | Date with time (default for DateTime) |
| `type_string()`   | TEXT            | Explicit string conversion            |
| `type_integer()`  | INTEGER         | Explicit integer conversion           |
| `type_float()`    | FLOAT8          | Explicit float conversion             |
| `type_boolean()`  | BOOLEAN         | Explicit boolean conversion           |

## Built-in Converters

The client includes converters for all common PostgreSQL types. Each converter maps between a Flow Type (for `typed()`)
and PostgreSQL types:

| Converter           | Flow Type                  | PostgreSQL Types            | PHP Return              |
|---------------------|----------------------------|-----------------------------|-------------------------|
| StringConverter     | `type_string()`            | TEXT, VARCHAR, CHAR, BPCHAR | `string`                |
| IntegerConverter    | `type_integer()`           | INT2, INT4, INT8            | `int`                   |
| FloatConverter      | `type_float()`             | FLOAT4, FLOAT8              | `float`                 |
| BooleanConverter    | `type_boolean()`           | BOOL                        | `bool`                  |
| DateTimeConverter   | `type_datetime()`          | TIMESTAMP, TIMESTAMPTZ      | `\DateTimeImmutable`    |
| DateConverter       | `type_date()`              | DATE                        | `\DateTimeImmutable`    |
| TimeConverter       | `type_datetime()`          | TIME, TIMETZ                | `\DateTimeImmutable`    |
| UuidConverter       | `type_uuid()`              | UUID                        | `Flow\Types\Value\Uuid` |
| JsonConverter       | `type_json()`              | JSON, JSONB                 | `Flow\Types\Value\Json` |
| ByteaConverter      | `type_string()`            | BYTEA                       | `string` (binary)       |
| ArrayConverter      | `type_list(type_string())` | *_ARRAY variants            | `array`                 |
| IntervalConverter   | `type_time()`              | INTERVAL                    | `\DateInterval`         |
| MultirangeConverter | `type_string()`            | (PostgreSQL 14+ multirange) | `string`                |

### Notes

- **UuidConverter** returns `Flow\Types\Value\Uuid` objects. Use `->toString()` to get string representation.
- **JsonConverter** returns `Flow\Types\Value\Json` objects. Use `->data()` to get the decoded array/object.
- **FloatConverter** handles special values: `Infinity`, `-Infinity`, and `NaN`.
- **ArrayConverter** handles all PostgreSQL array types (INT4[], TEXT[], UUID[], etc.).

## Custom ValueConverters

Implement `ValueConverter` for custom type handling:

```php
<?php

use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};
use Flow\Types\Type;

use function Flow\Types\DSL\type_object;

// Example: Custom Money type
readonly class Money
{
    public function __construct(
        public int $cents,
        public string $currency = 'USD',
    ) {}
}

readonly class MoneyConverter implements ValueConverter
{
    public function flowType(): Type
    {
        return type_object(Money::class);
    }

    public function supportedTypes(): array
    {
        return [PostgreSqlType::MONEY, PostgreSqlType::NUMERIC];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof Money) {
            return (string) ($value->cents / 100);
        }

        return (string) $value;
    }

    public function toPhp(string $value, PostgreSqlType $type): Money
    {
        // PostgreSQL MONEY format: $1,234.56
        $cleaned = preg_replace('/[^0-9.-]/', '', $value);
        $cents = (int) round((float) $cleaned * 100);

        return new Money($cents);
    }
}
```

## Registering Custom Converters

Add converters when creating the client:

```php
<?php

use Flow\PostgreSql\Client\Types\ValueConverters;

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

// Create converters with defaults + custom
$converters = ValueConverters::create()
    ->with(new MoneyConverter());

$client = pgsql_client(
    pgsql_connection('host=localhost dbname=mydb'),
    valueConverters: $converters,
);

// Now MONEY columns automatically convert to Money objects
$product = $client->fetch('SELECT price FROM products WHERE id = $1', [1]);
$price = $product['price'];  // Money instance
```

## ValueConverters Registry

The `ValueConverters` class manages type converters:

```php
<?php

use Flow\PostgreSql\Client\Types\{PostgreSqlType, PostgreSqlVersion, ValueConverters};

// Create with defaults for PostgreSQL version
$converters = ValueConverters::create(PostgreSqlVersion::V16);

// Add custom converter (returns new immutable instance)
$converters = $converters->with(new MoneyConverter());

// Check if converter exists for type
if ($converters->hasConverterFor(PostgreSqlType::UUID)) {
    // UUID converter is registered
}

// Get converter for specific PostgreSQL type
$converter = $converters->forPostgreSqlType(PostgreSqlType::JSONB);
```

## PostgreSQL Type OIDs

Types are identified by OID (Object ID) in PostgreSQL. The `PostgreSqlType` enum provides common OIDs:

```php
<?php

use Flow\PostgreSql\Client\Types\PostgreSqlType;

// Common types
PostgreSqlType::TEXT      // 25
PostgreSqlType::INT4      // 23
PostgreSqlType::INT8      // 20
PostgreSqlType::FLOAT8    // 701
PostgreSqlType::BOOL      // 16
PostgreSqlType::TIMESTAMP // 1114
PostgreSqlType::TIMESTAMPTZ // 1184
PostgreSqlType::UUID      // 2950
PostgreSqlType::JSON      // 114
PostgreSqlType::JSONB     // 3802
PostgreSqlType::BYTEA     // 17

// Array types
PostgreSqlType::TEXT_ARRAY // 1009
PostgreSqlType::INT4_ARRAY // 1007
```

## Handling NULL Values

NULL handling is consistent across all converters:

```php
<?php

// NULL in parameters
$client->execute(
    'INSERT INTO users (name, bio) VALUES ($1, $2)',
    ['John', null]  // bio will be NULL
);

// NULL in results
$user = $client->fetch('SELECT * FROM users WHERE id = $1', [1]);
if ($user['bio'] === null) {
    echo "No bio set";
}

// Typed NULL (when type matters for schema)
$client->execute(
    'INSERT INTO events (metadata) VALUES ($1)',
    [typed(null, type_json())]
);
```
