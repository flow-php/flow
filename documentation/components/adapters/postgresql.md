---
package: flow-php/etl-adapter-postgresql
---

# ETL Adapter: PostgreSQL

[PACKAGE_NAV]

[TOC]

The PostgreSQL adapter reads and writes PostgreSQL tables in ETL (Extract, Transform, Load) pipelines. It uses
the [PostgreSQL library](/documentation/components/libs/postgresql/client-connection.md) for extraction and loading.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/etl-adapter-postgresql.md).

## Requirements

- PHP 8.3+
- ext-pgsql
- ext-pg_query (optional, for query builder support)

## Description

This adapter has:

### Extractors

Three extraction strategies for different use cases:

- Server-Side Cursor: true streaming extraction with PostgreSQL DECLARE CURSOR, for the lowest memory usage
- LIMIT/OFFSET Pagination: simple pagination for smaller datasets
- Keyset (Cursor) Pagination: efficient pagination for large datasets, with the same performance on each page

All extractors support:

- Raw SQL strings or Query Builder objects
- Configurable batch/page sizes
- Maximum row limits
- Custom schema definitions

Each extractor reads exactly one read-only `SELECT` or `VALUES` statement. All other input throws
`InvalidArgumentException` before a query runs. For the schema derivation and its errors, read
[PostgreSQL sources](../core/schema.md#postgresql-sources-describe-themselves-and-keep-withschema).

### Loader

A loader that supports:

- INSERT: simple inserts with batch support
- UPDATE: update existing rows by primary key
- DELETE: delete rows by primary key
- UPSERT: ON CONFLICT handling for insert-or-update operations

## Extractor - Server-Side Cursor

The `from_pgsql_cursor` extractor uses the native server-side cursors of PostgreSQL through `DECLARE CURSOR` + `FETCH`.
This is the only way to get true low-memory streaming with the ext-pgsql of PHP, because the extension
has no unbuffered query mode.

> Note: This extractor manages transactions automatically. Cursors need a transaction context. Without an open
> transaction, the extractor starts one. A failed read rolls back the transaction that the extractor started.
> Inside a transaction that you opened, the rollback is your task.

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
| `from_pgsql_key_set`      | Large datasets with indexed keys    | Medium (page buffered) | Equal to the keys |
| `from_pgsql_limit_offset` | Small-medium datasets               | Medium (page buffered) | Yes               |

## Extractor - LIMIT/OFFSET Pagination

The `from_pgsql_limit_offset` extractor uses traditional LIMIT/OFFSET pagination. It is simple to use, but it can
become slow on very large datasets with high offsets.

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

The `from_pgsql_key_set` extractor uses keyset pagination (also known as cursor-based pagination). Each page has the
same performance, also deep in the dataset. It is therefore a good choice for large datasets.

### Key rule

The keys must be indexed, non-null and unique together. A single non-unique column such as `created_at` is not a
key. Add the primary key as the least significant key:

```php
pgsql_pagination_key_set(
    pgsql_pagination_key_asc('created_at'),
    pgsql_pagination_key_asc('id'),
);
```

- Before the first page, the extractor sends one check query:
  `SELECT 1 FROM (<query>) _flow_keyset_nulls WHERE created_at IS NULL OR id IS NULL LIMIT 1`. If it finds a row, it
  throws. On an indexed key, this is an index lookup. On a key without an index, it is one full scan.
- Two rows with the same key throw `Keyset pagination requires unique keys`.
- The next page must start at the row that the previous page looked ahead to. If it does not, the read throws.
  Two causes are possible. The database compares two different values as equal, for example with a case- or
  accent-insensitive collation, or `numeric` `1.0` and `1.00`. Or rows change between pages.
- Known limit: two such values on the same page are both read without an error. Nothing is lost.
- ORDER BY is generated from the keys. An ORDER BY in the query must be equal to the keys. The query must not have
  its own LIMIT or OFFSET. Use `withMaximum()` or `DataFrame::limit()`.
- A page shorter than the batch size ends the read without another query.

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

For tables with composite ordering, give multiple keys:

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

Each function returns an extractor. Configure it with the fluent `->withBatchSize(n)` (rows for each round trip, default 1000) and `->withMaximum(n)`.

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

### Bound values

`xml`, `xml_element`, `html` and `html_element` columns are bound as their markup text. An `xml` document is bound as
its root element only. The declaration, a DOCTYPE, and comments or processing instructions outside the root are
dropped.

| Column        | Value                                              | Bound                  |
|---------------|----------------------------------------------------|------------------------|
| `xml`         | `<?xml version="1.0"?><!-- c --><root><a/></root>` | `<root><a/></root>`    |
| `xml_element` | `<a b="1"><c/></a>`                                | `<a b="1"><c></c></a>` |

### Insert with Skip Conflicts (ON CONFLICT DO NOTHING)

Skip the rows that cause a constraint violation:

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

On conflict, update existing rows with specific columns:

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

Update the rows that match primary key values:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_update_options, to_pgsql_table};
use Flow\ETL\Adapter\PostgreSql\IO\Loader\Operation;

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

Delete the rows that match primary key values:

```php
use function Flow\ETL\Adapter\PostgreSql\{pgsql_delete_options, to_pgsql_table};
use Flow\ETL\Adapter\PostgreSql\IO\Loader\Operation;

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

`to_pgsql_transaction()` writes one or more children inside transactions. Each batch of rows is written in its own
transaction. If a child throws, the open transaction rolls back:

```php
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

Some branches have blocking operations: `to_transformation()`, also inside `to_branch()`, with `sortBy()`,
`aggregate()`, `groupBy()->aggregate()`, `pivot()`, window functions, `collect()` or `join()`. For more information,
read [transformations](../core/transformations.md). These branches buffer the stream and deliver it at the end of the
run. `to_pgsql_transaction()` opens one final transaction around that delivery. The full drained stream commits
atomically. A failure during the delivery rolls back, and `run()` throws it. A plain loader child is written directly.
A `to_transformation(...)` child delivers inside the same transaction. The loader of each child must use the same
`Client` instance as the transaction. A loader with its own `Client` is outside the transaction.

Inside the transaction, each child runs throw-only. A failure first rolls back the batch. Only after that, the
`onError()` handler of the frame decides about the rest of the run.

To set the isolation level, build the transaction yourself. The level applies to each transaction that opens, also
the final transaction:

```php
use Flow\ETL\Adapter\PostgreSql\IO\Loader\PostgreSqlTransaction;
use Flow\ETL\API\Transactional;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;

new Transactional(
    (new PostgreSqlTransaction($client))->withIsolationLevel(IsolationLevel::SERIALIZABLE),
    to_pgsql_table($client, 'users'),
);
```

## Loader DSL Functions Reference

| Function                                    | Description                                         |
|---------------------------------------------|-----------------------------------------------------|
| `to_pgsql_table($client, $table)`           | Create a PostgreSQL loader for a table              |
| `to_pgsql_transaction($client, ...$writes)` | Write children, every delivery inside a transaction |
| `pgsql_insert_options(...)`                 | Configure insert behavior (conflicts, upsert)       |
| `pgsql_update_options($primaryKeys)`        | Configure update behavior (primary key columns)     |
| `pgsql_delete_options($primaryKeys)`        | Configure delete behavior (primary key columns)     |

## Schema Conversion

Two helpers convert between a Flow `Schema` and a PostgreSQL table definition:

- `to_pgsql_schema_table()` turns a Flow `Schema` into a `Flow\PostgreSql\Schema\Table`. Its `toSql()` gives the
  `CREATE TABLE` SQL, and the related index/constraint SQL.
- `pgsql_table_to_flow_schema()` turns a `Flow\PostgreSql\Schema\Table` back into a Flow `Schema`.

The shared `TypesMap` resolves the column types (Flow type → PostgreSQL column type). `PostgreSqlMetadata` entries
on each schema definition control the details of each column: primary keys, unique constraints, indexes, length,
precision/scale, defaults, identity, generated columns, and explicit type overrides.

### Creating a Table from a Flow Schema

`to_pgsql_schema_table()` returns a table definition. To create the table, call `toSql()` on it and execute each
statement:

```php
use Flow\ETL\Adapter\PostgreSql\Values\Schema\PostgreSqlMetadata;

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

By default, the table is created in the `public` schema. For a different schema, give a third argument:

```php
$table = to_pgsql_schema_table($schema, 'users', 'analytics');
```

### Steering the Conversion with Metadata

`PostgreSqlMetadata` factories return `Metadata` objects. Attach them to a definition with the `metadata:` argument of
the schema DSL helpers. Combine multiple entries with `merge()`:

```php
use Flow\ETL\Adapter\PostgreSql\Values\Schema\PostgreSqlMetadata;
use Flow\PostgreSql\Schema\IdentityGeneration;

use function Flow\ETL\DSL\{float_schema, int_schema, schema, str_schema};

$schema = schema(
    int_schema('id', metadata: PostgreSqlMetadata::identity(IdentityGeneration::BY_DEFAULT)),
    str_schema('sku', metadata: PostgreSqlMetadata::type('citext')),                    // explicit type override
    float_schema('amount', metadata: PostgreSqlMetadata::precision(10)->merge(PostgreSqlMetadata::scale(2))),
    int_schema('total', metadata: PostgreSqlMetadata::generated('price * quantity')),
);
```

| Metadata                                            | Effect on the generated column                                                        |
|-----------------------------------------------------|---------------------------------------------------------------------------------------|
| `PostgreSqlMetadata::type($name)`                   | Force a specific PostgreSQL type, bypassing the type map                              |
| `PostgreSqlMetadata::length($n)`                    | Emit `varchar($n)`                                                                    |
| `PostgreSqlMetadata::precision($p)` / `::scale($s)` | Emit `numeric($p, $s)`                                                                |
| `PostgreSqlMetadata::default($v)`                   | Set a column `DEFAULT`                                                                |
| `PostgreSqlMetadata::primaryKey($name)`             | Include the column in the table primary key                                           |
| `PostgreSqlMetadata::indexUnique($name, $position)` | Include the column in a named `UNIQUE` constraint, optionally at an explicit position |
| `PostgreSqlMetadata::index($name, $position)`       | Include the column in a named index, optionally at an explicit position               |
| `PostgreSqlMetadata::identity($generation)`         | Make the column an identity column                                                    |
| `PostgreSqlMetadata::generated($expr)`              | Make the column a generated column                                                    |

Columns with the same primary key, unique constraint, or index name are grouped together. For a composite key,
attach the same name to several definitions.

#### Ordering Columns Within an Index

For composite indexes and unique constraints, the column order is important. By default, the columns have the order
of the schema. To set a different order, give an explicit `$position` (ascending, lower first) to `index()` /
`indexUnique()`. An example is a keyset-pagination index, where the first column must serve `ORDER BY`:

```php
use Flow\ETL\Adapter\PostgreSql\Values\Schema\PostgreSqlMetadata;

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

Columns without an explicit position go to the end (`PHP_INT_MAX`), and the schema order decides ties. Without
positions, the order is therefore the schema order.

#### A Column in Multiple Indexes

Each index has its own metadata key, so one column can belong to several indexes at the same time. To attach more
than one, chain `merge()`:

```php
use Flow\ETL\Adapter\PostgreSql\Values\Schema\PostgreSqlMetadata;

use function Flow\ETL\DSL\{datetime_schema, int_schema, schema};

$schema = schema(
    int_schema('id', metadata: PostgreSqlMetadata::primaryKey('pk_orders')
        ->merge(PostgreSqlMetadata::index('orders_created_at_id_idx', position: 2))),
    datetime_schema('created_at', metadata: PostgreSqlMetadata::index('orders_created_at_id_idx', position: 1)),
);
```

> Index and unique-constraint names must not contain a colon (`:`). Internally, the colon separates the name and
> the position. A name with a colon throws an `InvalidArgumentException`.

### Reading a Flow Schema back from a Table

`pgsql_table_to_flow_schema()` takes a `Flow\PostgreSql\Schema\Table` and returns a Flow `Schema`. Combine it with the
catalog provider of the PostgreSQL library to get a Flow schema from a live table:

```php
use function Flow\ETL\Adapter\PostgreSql\pgsql_table_to_flow_schema;
use function Flow\PostgreSql\DSL\client_catalog_provider;

$table = client_catalog_provider($client, ['public'])
    ->get()
    ->get('public')
    ->table('users');

$schema = pgsql_table_to_flow_schema($table);
```

> Note: The reverse conversion loses information deliberately. Several Flow types map to the same PostgreSQL type.
> For example, `json`, `list`, `map`, and `structure` all map to `jsonb`. A column therefore maps back to one
> canonical Flow type, not to its original type.

### Customizing the Type Mapping

Both helpers accept an optional `TypesMap`. Its second constructor argument overrides the Flow type → PostgreSQL
column type mapping. The first argument still overrides the value-binding types of the loader:

```php
use Flow\ETL\Adapter\PostgreSql\Values\Schema\TypesMap;
use Flow\PostgreSql\QueryBuilder\Schema\ColumnType;
use Flow\Types\Type\Native\StringType;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;

$table = to_pgsql_schema_table(
    $schema,
    'users',
    typesMap: new TypesMap([], [
        StringType::class => ColumnType::varchar(255),  // default strings to varchar(255) instead of text
    ]),
);
```

### Schema Conversion DSL Functions Reference

| Function                                                                 | Description                                       |
|--------------------------------------------------------------------------|---------------------------------------------------|
| `to_pgsql_schema_table($schema, $tableName, $databaseSchema, $typesMap)` | Convert a Flow `Schema` into a PostgreSQL `Table` |
| `pgsql_table_to_flow_schema($table, $typesMap)`                          | Convert a PostgreSQL `Table` into a Flow `Schema` |
