---
package: flow-php/etl-adapter-postgresql
---

# ETL Adapter: PostgreSQL

[PACKAGE_NAV]

[TOC]

Flow PHP's Adapter PostgreSQL is designed to seamlessly integrate PostgreSQL within your ETL (Extract, Transform, Load)
workflows. This adapter is built on top of
the [PostgreSQL library](/documentation/components/libs/postgresql/client-connection.md), providing efficient data
extraction and loading capabilities. By harnessing the Adapter PostgreSQL library, developers can tap into robust
features for precise database interaction, simplifying complex data transformations and enhancing data processing
efficiency.

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/etl-adapter-postgresql.md).

## Requirements

- PHP 8.3+
- ext-pgsql
- ext-pg_query (optional, for query builder support)

## Description

This adapter provides:

### Extractors

Three extraction strategies optimized for different use cases:

- **Server-Side Cursor**: True streaming extraction using PostgreSQL DECLARE CURSOR for maximum memory efficiency
- **LIMIT/OFFSET Pagination**: Simple pagination suitable for smaller datasets
- **Keyset (Cursor) Pagination**: Efficient pagination for large datasets with consistent performance

All extractors support:

- Raw SQL strings or Query Builder objects
- Configurable batch/page sizes
- Maximum row limits
- Custom schema definitions

### Loader

A flexible loader supporting:

- **INSERT**: Simple inserts with batch support
- **UPDATE**: Update existing rows by primary key
- **DELETE**: Delete rows by primary key
- **UPSERT**: ON CONFLICT handling for insert-or-update operations

## Extractor - Server-Side Cursor

The `from_pgsql_cursor` extractor uses PostgreSQL's native server-side cursors via `DECLARE CURSOR` + `FETCH`.
This is the **only way** to achieve true low-memory streaming with PHP's ext-pgsql, as the extension
has no unbuffered query mode.

> **Note:** This extractor automatically manages transactions. Cursors require a transaction context,
> which is auto-started if not already in one.

### Basic Usage

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$client = pgsql_client(pgsql_connection_dsn('pgsql://user:pass@localhost:5432/database'));

data_frame()
    ->read(from_pgsql_cursor(
        $client,
        "SELECT id, name, email FROM users",
    )->withBatchSize(1000))
    ->write(to_output())
    ->run();
```

### With Query Builder

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;
use function Flow\PostgreSql\DSL\{col, select, star, table};

data_frame()
    ->read(from_pgsql_cursor(
        $client,
        select(star())->from(table('large_table')),
    )->withBatchSize(500))
    ->write(to_output())
    ->run();
```

### With Parameters

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_cursor;

data_frame()
    ->read(from_pgsql_cursor(
        $client,
        "SELECT * FROM orders WHERE status = $1 AND created_at > $2",
        parameters: ['pending', '2024-01-01'],
    )->withBatchSize(1000))
    ->write(to_output())
    ->run();
```

### When to Use Each Extractor

| Extractor                 | Best For                            | Memory                 | ORDER BY Required |
|---------------------------|-------------------------------------|------------------------|-------------------|
| `from_pgsql_cursor`       | Very large datasets, true streaming | Lowest (server-side)   | No                |
| `from_pgsql_key_set`      | Large datasets with indexed keys    | Medium (page buffered) | Auto-generated    |
| `from_pgsql_limit_offset` | Small-medium datasets               | Medium (page buffered) | Yes               |

## Extractor - LIMIT/OFFSET Pagination

The `from_pgsql_limit_offset` extractor uses traditional LIMIT/OFFSET pagination. This is simple to use but may have
performance degradation on very large datasets with high offsets.

### Basic Usage

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$client = pgsql_client(pgsql_connection_dsn('pgsql://user:pass@localhost:5432/database'));

data_frame()
    ->read(from_pgsql_limit_offset(
        $client,
        "SELECT id, name, email FROM users ORDER BY id",
    )->withBatchSize(1000))
    ->write(to_output())
    ->run();
```

### With Query Builder

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\PostgreSql\DSL\{asc, col, select, table};

data_frame()
    ->read(from_pgsql_limit_offset(
        $client,
        select(col('id'), col('name'), col('email'))
            ->from(table('users'))
            ->orderBy(asc(col('id'))),
    )->withBatchSize(500))
    ->write(to_output())
    ->run();
```

### With Maximum Row Limit

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;

data_frame()
    ->read(from_pgsql_limit_offset(
        $client,
        "SELECT * FROM large_table ORDER BY id",
    )->withBatchSize(1000)->withMaximum(10000)) // Only extract first 10,000 rows
    ->write(to_output())
    ->run();
```

## Extractor - Keyset (Cursor) Pagination

The `from_pgsql_key_set` extractor uses keyset pagination (also known as cursor-based pagination). This provides
consistent performance regardless of how deep you paginate, making it ideal for large datasets.

> **Note:** The ORDER BY clause is automatically generated from the keyset configuration. You only need to define
> the sort order once using `pgsql_pagination_key_asc()` or `pgsql_pagination_key_desc()`.

### Basic Usage

```php
use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_set};

data_frame()
    ->read(from_pgsql_key_set(
        $client,
        "SELECT id, name, email FROM users",
        pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
    )->withBatchSize(1000))
    ->write(to_output())
    ->run();
```

### Descending Order

```php
use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_desc, pgsql_pagination_key_set};

data_frame()
    ->read(from_pgsql_key_set(
        $client,
        "SELECT id, name, created_at FROM orders",
        pgsql_pagination_key_set(pgsql_pagination_key_desc('id')),  // Newest first
    )->withBatchSize(500))
    ->write(to_output())
    ->run();
```

### Composite Keys

For tables with composite ordering, you can specify multiple keys:

```php
use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_desc, pgsql_pagination_key_set};

data_frame()
    ->read(from_pgsql_key_set(
        $client,
        "SELECT * FROM events",
        pgsql_pagination_key_set(
            pgsql_pagination_key_desc('created_at'),  // First by date descending
            pgsql_pagination_key_asc('id')             // Then by ID ascending
        ),
    )->withBatchSize(1000))
    ->write(to_output())
    ->run();
```

### With Query Builder

```php
use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_set};
use function Flow\PostgreSql\DSL\{col, select, star, table};

data_frame()
    ->read(from_pgsql_key_set(
        $client,
        select(star())->from(table('products')),
        pgsql_pagination_key_set(pgsql_pagination_key_asc('product_id')),
    )->withBatchSize(500))
    ->write(to_output())
    ->run();
```

## DSL Functions Reference

### Extractor Functions

| Function                                                    | Description                           |
|-------------------------------------------------------------|---------------------------------------|
| `from_pgsql_cursor($client, $query, $parameters)`           | Extract using server-side cursor      |
| `from_pgsql_limit_offset($client, $query, $parameters)`     | Extract using LIMIT/OFFSET pagination |
| `from_pgsql_key_set($client, $query, $keySet, $parameters)` | Extract using keyset pagination       |

Each returns an extractor configured with the fluent `->withBatchSize(n)` (rows per round trip, default 1000) and `->withMaximum(n)`.

### Key Functions

| Function                             | Description                                   |
|--------------------------------------|-----------------------------------------------|
| `pgsql_pagination_key_asc($column)`  | Create an ascending key for keyset pagination |
| `pgsql_pagination_key_desc($column)` | Create a descending key for keyset pagination |
| `pgsql_pagination_key_set(...$keys)` | Create a keyset from one or more keys         |

## Loader

The `to_pgsql_table` loader writes data to PostgreSQL tables. It supports INSERT, UPDATE, and DELETE operations with
configurable conflict handling.

### Basic Insert

```php
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\DSL\{df, from_array};
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$client = pgsql_client(pgsql_connection_dsn('pgsql://user:pass@localhost:5432/database'));

df()
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice', 'email' => 'alice@example.com'],
        ['id' => 2, 'name' => 'Bob', 'email' => 'bob@example.com'],
    ]))
    ->write(to_pgsql_table($client, 'users'))
    ->run();
```

### Insert with Skip Conflicts (ON CONFLICT DO NOTHING)

Skip rows that would cause a constraint violation:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_insert_options, to_pgsql_table};

df()
    ->read(from_array($data))
    ->write(
        to_pgsql_table($client, 'users')
            ->withInsertOptions(pgsql_insert_options(skipConflicts: true))
    )
    ->run();
```

### Upsert (ON CONFLICT DO UPDATE)

Update existing rows on conflict using specific columns:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_insert_options, to_pgsql_table};

df()
    ->read(from_array($data))
    ->write(
        to_pgsql_table($client, 'users')
            ->withInsertOptions(pgsql_insert_options(
                conflictColumns: ['email'],        // Detect conflicts on these columns
                updateColumns: ['name', 'updated_at']  // Update these columns on conflict
            ))
    )
    ->run();
```

### Upsert on Constraint

Use a named constraint for conflict detection:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_insert_options, to_pgsql_table};

df()
    ->read(from_array($data))
    ->write(
        to_pgsql_table($client, 'users')
            ->withInsertOptions(pgsql_insert_options(
                conflictConstraint: 'users_email_key',
                updateColumns: ['name']
            ))
    )
    ->run();
```

### Update Existing Rows

Update rows matching primary key values:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_update_options, to_pgsql_table};
use Flow\ETL\Adapter\PostgreSql\Operation;

df()
    ->read(from_array([
        ['id' => 1, 'name' => 'Alice Updated', 'email' => 'alice_new@example.com'],
        ['id' => 2, 'name' => 'Bob Updated', 'email' => 'bob_new@example.com'],
    ]))
    ->write(
        to_pgsql_table($client, 'users')
            ->withOperation(Operation::UPDATE)
            ->withUpdateOptions(pgsql_update_options(['id']))  // Match on 'id' column
    )
    ->run();
```

### Delete Rows

Delete rows matching primary key values:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_delete_options, to_pgsql_table};
use Flow\ETL\Adapter\PostgreSql\Operation;

df()
    ->read(from_array([
        ['id' => 1],
        ['id' => 3],
    ]))
    ->write(
        to_pgsql_table($client, 'users')
            ->withOperation(Operation::DELETE)
            ->withDeleteOptions(pgsql_delete_options(['id']))
    )
    ->run();
```

### Transactional Loading

`to_pgsql_transaction()` wraps one or more loaders so every delivery happens inside a transaction: each batch of rows
is loaded in its own transaction, and if any loader throws, the open transaction is rolled back:

```php
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

use function Flow\ETL\Adapter\PostgreSql\{to_pgsql_table, to_pgsql_transaction};

df()
    ->read(from_array($data))
    ->write(to_pgsql_transaction(
        $client,
        to_pgsql_table($client, 'users'),
        to_pgsql_table($client, 'users_audit'),
    ))
    ->run();
```

Wrapped `to_transformation()` / `to_branch(...)->withTransformation(...)` steps with blocking operations (`sortBy()`,
`aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()`, `join()` - see
[transformations](../core/transformations.md)) buffer the stream and deliver it when the pipeline closes the loader;
`to_pgsql_transaction()` opens one final transaction around that delivery - the whole drained stream commits
atomically, a failure during it rolls back. Every wrapped loader must use the same `Client` instance as the wrapper -
a loader holding its own `Client` escapes the transaction.

Do not place `write_with_retries()` inside the wrapper: after a failed statement PostgreSQL aborts the whole
transaction, so every retry attempt fails too. Wrap the transaction instead -
`write_with_retries(to_pgsql_transaction(...))` gives each attempt a fresh transaction (see
[retry](../core/retry.md)).

Use `withIsolationLevel()` to set the transaction isolation level; it applies to every transaction the wrapper opens,
including the final one:

```php
to_pgsql_transaction($client, to_pgsql_table($client, 'users'))
    ->withIsolationLevel(IsolationLevel::SERIALIZABLE);
```

## Loader DSL Functions Reference

| Function                                       | Description                                               |
|------------------------------------------------|-----------------------------------------------------------|
| `to_pgsql_table($client, $table)`              | Create a PostgreSQL loader for a table                    |
| `to_pgsql_transaction($client, ...$loaders)`   | Run multiple loaders, every delivery inside a transaction |
| `pgsql_insert_options(...)`                    | Configure insert behavior (conflicts, upsert)             |
| `pgsql_update_options($primaryKeys)`           | Configure update behavior (primary key columns)           |
| `pgsql_delete_options($primaryKeys)`           | Configure delete behavior (primary key columns)           |

## Schema Conversion

Two helpers convert between a Flow `Schema` and a PostgreSQL table definition:

- `to_pgsql_schema_table()` turns a Flow `Schema` into a `Flow\PostgreSql\Schema\Table`, which can emit `CREATE TABLE`
  (and related index/constraint) SQL via `toSql()`.
- `pgsql_table_to_flow_schema()` turns a `Flow\PostgreSql\Schema\Table` back into a Flow `Schema`.

Column types are resolved through the shared `EntryTypesMap` (Flow type → PostgreSQL column type), and per-column
details - primary keys, unique constraints, indexes, length, precision/scale, defaults, identity, generated columns,
and explicit type overrides - are driven by `PostgreSqlMetadata` entries attached to each schema definition.

### Creating a Table from a Flow Schema

`to_pgsql_schema_table()` returns a table definition; call `toSql()` on it and execute each statement to create the
table:

```php
use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;
use function Flow\ETL\DSL\{bool_schema, int_schema, json_schema, schema, str_schema};

$table = to_pgsql_schema_table(
    schema(
        int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_users')),
        str_schema('name', metadata: PostgreSqlMetadata::length(120)),
        str_schema('email', metadata: PostgreSqlMetadata::indexUnique('uq_users_email')),
        bool_schema('active', metadata: PostgreSqlMetadata::default(true)),
        json_schema('payload'),
    ),
    'users',
);

foreach ($table->toSql() as $sql) {
    $client->execute($sql);
}
```

By default the table is created in the `public` schema; pass a third argument to target another one:

```php
$table = to_pgsql_schema_table($schema, 'users', 'analytics');
```

### Steering the Conversion with Metadata

`PostgreSqlMetadata` factories return `Metadata` objects you attach to a definition via the `metadata:` argument of the
schema DSL helpers. Combine multiple entries with `merge()`:

```php
use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;
use Flow\PostgreSql\Schema\IdentityGeneration;

use function Flow\ETL\DSL\{float_schema, int_schema, schema, str_schema};

$schema = schema(
    int_schema('id', metadata: PostgreSqlMetadata::identity(IdentityGeneration::BY_DEFAULT)),
    str_schema('sku', metadata: PostgreSqlMetadata::type('citext')),                    // explicit type override
    float_schema('amount', metadata: PostgreSqlMetadata::precision(10)->merge(PostgreSqlMetadata::scale(2))),
    int_schema('total', metadata: PostgreSqlMetadata::generated('price * quantity')),
);
```

| Metadata                          | Effect on the generated column                             |
|-----------------------------------|------------------------------------------------------------|
| `PostgreSqlMetadata::type($name)` | Force a specific PostgreSQL type, bypassing the type map   |
| `PostgreSqlMetadata::length($n)`  | Emit `varchar($n)`                                         |
| `PostgreSqlMetadata::precision($p)` / `::scale($s)` | Emit `numeric($p, $s)`                   |
| `PostgreSqlMetadata::default($v)` | Set a column `DEFAULT`                                      |
| `PostgreSqlMetadata::primaryKey($name)` | Include the column in the table primary key          |
| `PostgreSqlMetadata::indexUnique($name, $position)` | Include the column in a named `UNIQUE` constraint, optionally at an explicit position |
| `PostgreSqlMetadata::index($name, $position)` | Include the column in a named index, optionally at an explicit position |
| `PostgreSqlMetadata::identity($generation)` | Make the column an identity column               |
| `PostgreSqlMetadata::generated($expr)` | Make the column a generated column                    |

Columns sharing the same primary key, unique constraint, or index name are grouped together, so composite keys are
expressed by attaching the same name to several definitions.

#### Ordering Columns Within an Index

For composite indexes and unique constraints, column order matters. By default columns are ordered the way they appear
in the schema. Pass an explicit `$position` (ascending, lower first) to `index()` / `indexUnique()` to control the
order independently of schema field order - useful, for example, for a keyset-pagination index where the leading
column must serve `ORDER BY`:

```php
use Flow\ETL\Adapter\PostgreSql\PostgreSqlMetadata;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;
use function Flow\ETL\DSL\{datetime_schema, int_schema, schema};

// `id` is field 0, `created_at` is field 1, but the index must lead with `created_at`.
$table = to_pgsql_schema_table(
    schema(
        int_schema('id', metadata: PostgreSqlMetadata::index('orders_created_at_id_idx', position: 2)),
        datetime_schema('created_at', metadata: PostgreSqlMetadata::index('orders_created_at_id_idx', position: 1)),
    ),
    'orders',
);

// => CREATE INDEX orders_created_at_id_idx ON public.orders (created_at, id)
```

Columns without an explicit position default to the end (`PHP_INT_MAX`), and schema order breaks ties - so leaving
positions off keeps the schema-order behavior.

#### A Column in Multiple Indexes

Because each index is tracked under its own metadata key, a single column can belong to several indexes at once. Chain
`merge()` to attach more than one:

```php
$schema = schema(
    int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_orders')
        ->merge(PostgreSqlMetadata::index('orders_created_at_id_idx', position: 2))),
    datetime_schema('created_at', metadata: PostgreSqlMetadata::index('orders_created_at_id_idx', position: 1)),
);
```

> Index and unique-constraint names must not contain a colon (`:`) - it is reserved internally as the name/position
> separator and passing one throws an `InvalidArgumentException`.

### Reading a Flow Schema back from a Table

`pgsql_table_to_flow_schema()` takes a `Flow\PostgreSql\Schema\Table` and returns a Flow `Schema`. Combine it with the
PostgreSQL library's catalog provider to derive a Flow schema from a live table:

```php
use function Flow\ETL\Adapter\PostgreSql\pgsql_table_to_flow_schema;
use function Flow\PostgreSql\DSL\client_catalog_provider;

$table = client_catalog_provider($client, ['public'])
    ->get()
    ->get('public')
    ->table('users');

$schema = pgsql_table_to_flow_schema($table);
```

> **Note:** The reverse conversion is intentionally lossy. Several Flow types collapse onto the same PostgreSQL type
> (for example `json`, `list`, `map`, and `structure` all map to `jsonb`), so a column is mapped back to a single
> canonical Flow type rather than its original one.

### Customizing the Type Mapping

Both helpers accept an optional `EntryTypesMap`. Its second constructor argument overrides the Flow type → PostgreSQL
column type mapping (the first argument keeps overriding the value-binding types used by the loader):

```php
use Flow\ETL\Adapter\PostgreSql\EntryTypesMap;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;

$table = to_pgsql_schema_table(
    $schema,
    'users',
    typesMap: new EntryTypesMap([], [
        StringType::class => ColumnType::varchar(255),  // default strings to varchar(255) instead of text
    ]),
);
```

### Schema Conversion DSL Functions Reference

| Function                                                              | Description                                            |
|-----------------------------------------------------------------------|--------------------------------------------------------|
| `to_pgsql_schema_table($schema, $tableName, $databaseSchema, $typesMap)` | Convert a Flow `Schema` into a PostgreSQL `Table`   |
| `pgsql_table_to_flow_schema($table, $typesMap)`                       | Convert a PostgreSQL `Table` into a Flow `Schema`      |
