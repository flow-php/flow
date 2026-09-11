# Type System

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The client handles type conversion between PHP and PostgreSQL in two directions:

- **Parameter binding**: Converting PHP values to PostgreSQL format when executing queries
- **Result casting**: Converting PostgreSQL results back to PHP types

## Result Casting

When fetching data, PostgreSQL returns all values as strings. The client automatically casts **unambiguous types** to
their
native PHP equivalents:

| PostgreSQL Type | PHP Type          | Notes                                                 |
|-----------------|-------------------|-------------------------------------------------------|
| BOOL            | `bool`            | `'t'` → `true`, `'f'` → `false`                       |
| INT2, INT4      | `int`             | smallint, integer                                     |
| INT8            | `int` or `string` | `int` on 64-bit, `string` on 32-bit to avoid overflow |
| FLOAT4, FLOAT8  | `float`           | Handles `Infinity`, `-Infinity`, `NaN`                |
| BYTEA           | `string`          | Decoded binary data                                   |
| Everything else | `string`          | JSON, UUID, dates, arrays, etc.                       |

**Why only these types?** Types like JSON, UUID, timestamps, and arrays have multiple valid PHP representations.
Rather than choosing one, the client returns them as strings, letting you parse them as needed:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// JSON comes back as string - parse it yourself
$row = $client->fetch('SELECT metadata FROM events WHERE id = $1', [1]);
$metadata = json_decode($row['metadata'], associative: true);

// UUID comes back as string
$row = $client->fetch('SELECT id FROM users WHERE email = $1', ['john@example.com']);
$uuid = $row['id'];  // '550e8400-e29b-41d4-a716-446655440000'

// Timestamps come back as strings
$row = $client->fetch('SELECT created_at FROM users WHERE id = $1', [1]);
$createdAt = new \DateTimeImmutable($row['created_at']);
```

## Parameter Binding

PHP values are automatically converted when binding parameters:

| PHP Type             | PostgreSQL Type           | Notes                                                                             |
|----------------------|---------------------------|-----------------------------------------------------------------------------------|
| `string`             | TEXT                      | Default for strings                                                               |
| `int`                | INTEGER (INT4)            |                                                                                   |
| `float`              | DOUBLE PRECISION (FLOAT8) |                                                                                   |
| `bool`               | BOOLEAN                   | `true` → `'t'`, `false` → `'f'`                                                   |
| `null`               | NULL                      |                                                                                   |
| `\DateTimeInterface` | TIMESTAMP                 | Normalized to UTC; use `typed()` with `ValueType::TIMESTAMPTZ` to keep the offset |
| `array`              | Ambiguous              | Use `typed()` to specify JSON or ARRAY                                            |

### The Array Ambiguity

Plain arrays cannot be bound directly because PostgreSQL has both JSON and native ARRAY types:

```php
<?php

// This will throw ValueConversionException
$client->execute('INSERT INTO events (data) VALUES ($1)', [['key' => 'value']]);
// Error: Cannot determine array type - use typed() to specify JSON or ARRAY

// Use typed() to be explicit
use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\ValueType;

// As JSON
$client->execute(
    'INSERT INTO events (data) VALUES ($1)',
    [typed(['key' => 'value'], ValueType::JSON)]
);

// As PostgreSQL array
$client->execute(
    'INSERT INTO tags (names) VALUES ($1)',
    [typed(['php', 'postgresql'], ValueType::TEXT_ARRAY)]
);
```

## Explicit Type Hints with typed()

When automatic detection isn't sufficient, use `typed()` to specify the exact PostgreSQL type:

```php
<?php

use function Flow\PostgreSql\DSL\{
    pgsql_client, pgsql_connection, typed,
    value_type_uuid, value_type_json, value_type_date, value_type_int4_array
};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// UUID - string that should be treated as UUID
$client->fetch(
    'SELECT * FROM users WHERE id = $1',
    [typed('550e8400-e29b-41d4-a716-446655440000', value_type_uuid())]
);

// JSON - array to be sent as JSON
$client->execute(
    'INSERT INTO events (payload) VALUES ($1)',
    [typed(['event' => 'login', 'user_id' => 42], value_type_json())]
);

// DATE - DateTime that should be DATE, not TIMESTAMP
$client->fetch(
    'SELECT * FROM events WHERE date = $1',
    [typed(new \DateTimeImmutable('2024-01-15'), value_type_date())]
);

// Integer array
$client->execute(
    'INSERT INTO scores (values) VALUES ($1)',
    [typed([100, 200, 300], value_type_int4_array())]
);
```

You can also use the `ValueType` enum directly if you prefer:

```php
<?php

use function Flow\PostgreSql\DSL\typed;
use Flow\PostgreSql\Client\Types\ValueType;

$client->execute(
    'INSERT INTO events (payload) VALUES ($1)',
    [typed(['event' => 'login'], ValueType::JSON)]
);
```

### Type DSL Functions

| DSL Function                                  | PostgreSQL Type  | Use Case                       |
|-----------------------------------------------|------------------|--------------------------------|
| **String types**                              |                  |                                |
| `value_type_text()`                           | TEXT             | Text strings                   |
| `value_type_varchar()`                        | VARCHAR          | Variable-length strings        |
| `value_type_char()`                           | CHAR             | Fixed-length strings           |
| **Integer types**                             |                  |                                |
| `value_type_int2()` / `value_type_smallint()` | SMALLINT         | Small integers                 |
| `value_type_int4()` / `value_type_integer()`  | INTEGER          | Standard integers              |
| `value_type_int8()` / `value_type_bigint()`   | BIGINT           | Large integers                 |
| **Floating point types**                      |                  |                                |
| `value_type_float4()` / `value_type_real()`   | REAL             | Single precision floats        |
| `value_type_float8()` / `value_type_double()` | DOUBLE PRECISION | Double precision floats        |
| `value_type_numeric()`                        | NUMERIC          | Arbitrary precision numbers    |
| `value_type_money()`                          | MONEY            | Currency amounts               |
| **Boolean type**                              |                  |                                |
| `value_type_bool()` / `value_type_boolean()`  | BOOLEAN          | True/false values              |
| **Binary types**                              |                  |                                |
| `value_type_bytea()`                          | BYTEA            | Binary data                    |
| `value_type_bit()`                            | BIT              | Bit strings                    |
| `value_type_varbit()`                         | VARBIT           | Variable-length bit strings    |
| **Date/time types**                           |                  |                                |
| `value_type_date()`                           | DATE             | Dates without time             |
| `value_type_time()`                           | TIME             | Time without timezone          |
| `value_type_timetz()`                         | TIMETZ           | Time with timezone             |
| `value_type_timestamp()`                      | TIMESTAMP        | Timestamp without timezone     |
| `value_type_timestamptz()`                    | TIMESTAMPTZ      | Timestamp with timezone        |
| `value_type_interval()`                       | INTERVAL         | Time intervals                 |
| **JSON types**                                |                  |                                |
| `value_type_json()`                           | JSON             | JSON data                      |
| `value_type_jsonb()`                          | JSONB            | Binary JSON data               |
| **UUID type**                                 |                  |                                |
| `value_type_uuid()`                           | UUID             | Universally unique identifiers |
| **Network types**                             |                  |                                |
| `value_type_inet()`                           | INET             | IPv4/IPv6 addresses            |
| `value_type_cidr()`                           | CIDR             | Network addresses              |
| `value_type_macaddr()`                        | MACADDR          | MAC addresses                  |
| `value_type_macaddr8()`                       | MACADDR8         | MAC addresses (EUI-64)         |
| **Other types**                               |                  |                                |
| `value_type_xml()`                            | XML              | XML data                       |
| `value_type_oid()`                            | OID              | Object identifiers             |
| **Array types**                               |                  |                                |
| `value_type_text_array()`                     | TEXT[]           | Array of strings               |
| `value_type_int4_array()`                     | INTEGER[]        | Array of integers              |
| `value_type_int8_array()`                     | BIGINT[]         | Array of big integers          |
| `value_type_float8_array()`                   | FLOAT8[]         | Array of floats                |
| `value_type_bool_array()`                     | BOOLEAN[]        | Array of booleans              |
| `value_type_uuid_array()`                     | UUID[]           | Array of UUIDs                 |
| `value_type_json_array()`                     | JSON[]           | Array of JSON                  |
| `value_type_jsonb_array()`                    | JSONB[]          | Array of JSONB                 |

## Built-in Value Converters

The client includes converters for all common PostgreSQL types. These converters handle the `toDatabase()` conversion
when you use `typed()`:

| Converter            | PostgreSQL Types            | PHP Input Types             |
|----------------------|-----------------------------|-----------------------------|
| StringConverter      | TEXT, VARCHAR, CHAR, BPCHAR | `string`                    |
| IntegerConverter     | INT2, INT4, INT8            | `int`                       |
| FloatConverter       | FLOAT4, FLOAT8              | `float`                     |
| BooleanConverter     | BOOL                        | `bool`                      |
| TimestampConverter   | TIMESTAMP                   | `\DateTimeInterface` (UTC)  |
| TimestampTzConverter | TIMESTAMPTZ                 | `\DateTimeInterface`        |
| DateConverter        | DATE                        | `\DateTimeInterface`        |
| TimeConverter        | TIME, TIMETZ                | `\DateTimeInterface`        |
| UuidConverter        | UUID                        | `string`                    |
| JsonConverter        | JSON, JSONB                 | `array`, `object`, `string` |
| ByteaConverter       | BYTEA                       | `string` (binary)           |
| BoolArrayConverter   | BOOL[]                      | `array`                     |
| IntArrayConverter    | INT2[], INT4[], INT8[]      | `array`                     |
| FloatArrayConverter  | FLOAT4[], FLOAT8[]          | `array`                     |
| TextArrayConverter   | TEXT[], VARCHAR[]           | `array`                     |
| UuidArrayConverter   | UUID[]                      | `array`                     |
| JsonArrayConverter   | JSON[], JSONB[]             | `array`                     |
| IntervalConverter    | INTERVAL                    | `\DateInterval`, `string`   |
| NumericConverter     | NUMERIC                     | `string`, `int`, `float`    |
| MoneyConverter       | MONEY                       | `string`, `int`, `float`    |
| InetConverter        | INET                        | `string`                    |
| CidrConverter        | CIDR                        | `string`                    |

### Converter Notes

- **FloatConverter**: Handles special PostgreSQL float values (`Infinity`, `-Infinity`, `NaN`)
- **Array Converters**: Type-specific converters for PostgreSQL arrays. Each validates element types strictly:
    - `BoolArrayConverter`: Expects `bool` elements
    - `IntArrayConverter`: Expects `int` elements
    - `FloatArrayConverter`: Expects `float` elements
    - `TextArrayConverter`: Expects `string` or scalar elements
    - `UuidArrayConverter`: Expects `string` elements (UUID format)
    - `JsonArrayConverter`: Expects `array` or `object` elements (JSON-encodable)
    - All converters support `null` elements (converted to PostgreSQL `NULL`)
    - Invalid element types throw `ValueConversionException`
- **JsonConverter**: Accepts PHP arrays, objects, or already-encoded JSON strings
- **NumericConverter**: Preserves precision as string to avoid floating-point errors

## Custom ValueConverters

Implement `ValueConverter` for custom type handling:

```php
<?php

use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

// Example: Custom Money type
readonly class Money
{
    public function __construct(
        public int $cents,
        public string $currency = 'USD',
    ) {}
}

readonly class CustomMoneyConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::MONEY, ValueType::NUMERIC];
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
}
```

## Registering Custom Converters

Add converters when creating the client:

```php
<?php

use Flow\PostgreSql\Client\Types\ValueConverters;

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

// Create converters with defaults + register a custom one
$converters = ValueConverters::create();
$converters->register(new CustomMoneyConverter());

$client = pgsql_client(
    pgsql_connection('host=localhost dbname=mydb'),
    valueConverters: $converters,
);
```

## ValueConverters Registry

The `ValueConverters` class manages type converters:

```php
<?php

use Flow\PostgreSql\Client\Types\{PostgreSqlVersion, ValueConverters, ValueType};

// Create with defaults for PostgreSQL version
$converters = ValueConverters::create(PostgreSqlVersion::V16);

// Register a custom converter (mutates the registry in place)
$converters->register(new CustomMoneyConverter());

// Check if a converter exists for a type
if ($converters->has(ValueType::UUID)) {
    // UUID converter is registered
}

// Get converter for a specific PostgreSQL type
$converter = $converters->forValueType(ValueType::JSONB);

// Remove a converter
$converters->unregister(ValueType::MONEY);
```

## Pre-converted Parameters

`execute()` runs a converter for every parameter. Parameters already in PostgreSQL's text form can skip that step:
wrap the whole list in `converted_parameters()` and every value is sent as it is.

```php
<?php

use function Flow\PostgreSql\DSL\converted_parameters;

$client->execute(
    'INSERT INTO users (id, created_at, tags) VALUES ($1, $2, $3)',
    converted_parameters(['1', '2024-01-01 00:00:00+00', '{a,b}']),
);
```

Bulk writes use it to resolve each column's converter once, instead of once per value:

```php
<?php

use function Flow\PostgreSql\DSL\{bulk_insert, converted_parameters, value_type_int8, value_type_timestamptz};

$converters = $client->converters();
$id = $converters->forValueType(value_type_int8());
$createdAt = $converters->forValueType(value_type_timestamptz());

$values = [];

foreach ($users as $user) {
    $values[] = $id->toDatabase($user->id);               // '42'
    $values[] = $createdAt->toDatabase($user->createdAt); // '2024-01-01 10:00:00.000000+00:00'
}

$client->execute(bulk_insert('users', ['id', 'created_at'], count($users)), converted_parameters($values));
```

- Each value is a `string`, or `null` for SQL `NULL`, bound by position to `$1`, `$2`, ...
- Nothing is converted or checked: registered converters are skipped, and a value PostgreSQL cannot parse for its
  column fails the query with a `QueryException` (SQLSTATE `22P02`).
- Only `execute()` accepts it.

## PostgreSQL Type Reference

PostgreSQL types are identified by OID (Object ID). The `ValueType` enum exposes the common OIDs used by the
client's converters and by `typed()`:

```php ignore
<?php

use Flow\PostgreSql\Client\Types\ValueType;

// Scalar types
ValueType::TEXT      // 25
ValueType::INT4      // 23
ValueType::INT8      // 20
ValueType::FLOAT8    // 701
ValueType::BOOL      // 16
ValueType::BYTEA     // 17

// Date/time types
ValueType::TIMESTAMP   // 1114
ValueType::TIMESTAMPTZ // 1184
ValueType::DATE        // 1082
ValueType::TIME        // 1083
ValueType::INTERVAL    // 1186

// Special types
ValueType::UUID      // 2950
ValueType::JSON      // 114
ValueType::JSONB     // 3802
ValueType::NUMERIC   // 1700
ValueType::MONEY     // 790

// Array types
ValueType::TEXT_ARRAY // 1009
ValueType::INT4_ARRAY // 1007
ValueType::UUID_ARRAY // 2951
```

## Handling NULL Values

NULL handling is consistent across all operations:

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

// Typed NULL (when type matters for schema validation)
$client->execute(
    'INSERT INTO events (metadata) VALUES ($1)',
    [typed(null, ValueType::JSON)]
);
```
