---
package: flow-php/postgresql
---

# PostgreSQL

[PACKAGE_NAV]

[TOC]

## Overview

> Note: This library is under active development. If you find an issue, for example in the Query Builder,
> [report a bug](https://github.com/flow-php/flow/issues/new?template=bug.yml).

The PostgreSQL library has three main features:

1. SQL Parser: parse, analyze, and modify existing PostgreSQL queries with the real PostgreSQL
   parser ([libpg_query](https://github.com/pganalyze/libpg_query))
2. Query Builder: build new queries programmatically with a fluent, type-safe API
3. Client: execute queries against PostgreSQL with automatic type conversion and object mapping

All features produce valid PostgreSQL syntax, and you can combine them. For example, build a query with the Query
Builder, execute it with the Client, and map the results to objects.

## Use Case Navigator

| I want to...                       | Go to                                                                              | Extension needed            |
|------------------------------------|------------------------------------------------------------------------------------|-----------------------------|
| Execute queries and fetch results  | [Client](#client)                                                                  | `ext-pgsql`                 |
| Build SQL queries with type safety | [Query Builder](#query-builder)                                                    | `ext-pg_query`              |
| Parse and analyze existing SQL     | [SQL Parser](#sql-parser)                                                          | `ext-pg_query`              |
| Add pagination to existing queries | [Query Modification](#query-modification)                                          | `ext-pg_query`              |
| Analyze query performance          | [Query Plan Analysis](/documentation/components/libs/postgresql/client-explain.md) | `ext-pgsql`, `ext-pg_query` |
| Traverse or modify AST directly    | [Advanced Features](#advanced-features)                                            | `ext-pg_query`              |
| Generate SQL from schema diffs     | [Schema Diff](#schema-diff)                                                        | `ext-pg_query`              |

## Requirements

This library has two optional PHP extensions. The features that you use decide which ones you need:

| Extension      | Required for                                          | Installation                                                                         |
|----------------|-------------------------------------------------------|--------------------------------------------------------------------------------------|
| `ext-pgsql`    | Client (database connections, query execution)        | Usually bundled with PHP, or `apt install php-pgsql`                                 |
| `ext-pg_query` | Query Builder, SQL Parser (AST parsing, manipulation) | [postgresql-ext documentation](/documentation/components/extensions/pg-query-ext.md) |

Both extensions are optional. You can use the Client without `ext-pg_query`, and the other features without `ext-pgsql`.

## Installation

For the installation instructions, read the [installation page](/documentation/installation/packages/postgresql.md).

---

## SQL Parser

Parse, analyze, and transform existing PostgreSQL queries.

### Quick Start

```php
<?php

use function Flow\PostgreSql\DSL\{sql_parse, sql_query_tables, sql_query_columns, sql_query_functions};

$query = sql_parse('SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id');

// Get all tables
foreach (sql_query_tables($query)->all() as $table) {
    echo $table->name();  // 'users', 'orders'
    echo $table->alias(); // 'u', 'o'
}

// Get all columns
foreach (sql_query_columns($query)->all() as $column) {
    echo $column->name();  // 'id', 'name', 'id', 'user_id'
    echo $column->table(); // 'u', 'u', 'u', 'o'
}

// Get columns for specific table
$userColumns = sql_query_columns($query)->forTable('u');

// Get all function calls
foreach (sql_query_functions($query)->all() as $func) {
    echo $func->name();   // function name
    echo $func->schema(); // schema if qualified (e.g., 'pg_catalog')
}
```

Extractors report each node of their kind at any position in the statement. This includes DDL targets
(`CREATE TABLE x AS ...`, `CREATE VIEW v ...`, `SELECT ... INTO t`), `excluded.*` and window `ORDER BY`. To get only
some of them, filter the result. `sql_query_tables()` returns only relations. It skips CTE references and
`FOR UPDATE OF` names. It includes the targets of `DROP`, `COMMENT ON`, `SECURITY LABEL` and `ALTER EXTENSION`.

### Parsing Utilities

```php
<?php

use function Flow\PostgreSql\DSL\{
    sql_parse,
    sql_fingerprint,
    sql_normalize,
    sql_normalize_utility,
    sql_split,
    sql_summary
};

// Parse SQL into ParsedQuery
$query = sql_parse('SELECT * FROM users WHERE id = 1');

// Generate fingerprint (same for structurally equivalent queries)
$fingerprint = sql_fingerprint('SELECT * FROM users WHERE id = 1');

// Normalize query (replace literals with positional parameters)
$normalized = sql_normalize("SELECT * FROM users WHERE name = 'John'");
// Returns: SELECT * FROM users WHERE name = $1

// Normalize also handles Doctrine-style named parameters
$normalized = sql_normalize('SELECT * FROM users WHERE id = :id');
// Returns: SELECT * FROM users WHERE id = $1

// Normalize utility/DDL statements
$normalized = sql_normalize_utility('CREATE TABLE users (id INT, name VARCHAR(255))');

// Split multiple statements
$statements = sql_split('SELECT 1; SELECT 2;');
// Returns: ['SELECT 1', ' SELECT 2']

// Generate query summary (protobuf format, useful for logging)
$summary = sql_summary('SELECT * FROM users WHERE id = 1');
```

### Deparsing (AST to SQL)

Convert a parsed query back to SQL, with optional pretty-printing:

```php
<?php

use function Flow\PostgreSql\DSL\{sql_parse, sql_deparse, sql_deparse_options, sql_format};

$query = sql_parse('SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true');

// Simple deparse (compact output)
$sql = sql_deparse($query);
// Returns: SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id WHERE u.active = true

// Pretty-printed output
$sql = sql_deparse($query, sql_deparse_options());
// Returns:
// SELECT u.id, u.name
// FROM
//     users u
//     JOIN orders o ON u.id = o.user_id
// WHERE u.active = true

// Custom formatting options
$sql = sql_deparse($query, sql_deparse_options()
    ->indentSize(2)           // 2 spaces per indent level
    ->maxLineLength(60)       // Wrap at 60 characters
    ->trailingNewline()       // Add newline at end
    ->commasStartOfLine()     // Place commas at line start
);

// Shorthand: parse and format in one step
$formatted = sql_format('SELECT id,name FROM users WHERE active=true');
```

#### DeparseOptions

| Method                    | Description                         | Default |
|---------------------------|-------------------------------------|---------|
| `prettyPrint(bool)`       | Enable/disable pretty printing      | `true`  |
| `indentSize(int)`         | Spaces per indentation level        | `4`     |
| `maxLineLength(int)`      | Maximum line length before wrapping | `80`    |
| `trailingNewline(bool)`   | Add trailing newline at end         | `false` |
| `commasStartOfLine(bool)` | Place commas at start of lines      | `false` |

---

## Query Builder

Build PostgreSQL queries programmatically with a fluent, type-safe API.

### Quick Start

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, col, table, literal, eq, asc
};

// Build a SELECT query
$query = select(col('id'), col('name'), col('email'))
    ->from(table('users'))
    ->where(eq(col('active'), literal(1)))
    ->orderBy(asc(col('name')))
    ->limit(10);

// Convert to SQL string
echo $query->toSQL();
// SELECT id, name, email FROM users WHERE active = 1 ORDER BY name LIMIT 10
```

```php
<?php

use function Flow\PostgreSql\DSL\{
    insert, col, param, conflict_columns, returning_all
};

// Build an upsert query
$query = insert()
    ->into('users')
    ->columns('email', 'name')
    ->values(param(1), param(2))
    ->onConflictDoUpdate(
        conflict_columns(['email']),
        ['name' => col('excluded.name')]
    )
    ->returningAll();

echo $query->toSQL();
// INSERT INTO users (email, name) VALUES ($1, $2)
// ON CONFLICT (email) DO UPDATE SET name = excluded.name RETURNING *
```

### Available Builders

Data Queries (DML)

- [Select Query Builder](/documentation/components/libs/postgresql/select-query-builder.md): SELECT with JOINs, CTEs,
  window functions, subqueries
- [Insert Query Builder](/documentation/components/libs/postgresql/insert-query-builder.md): INSERT with ON CONFLICT (
  upsert), RETURNING
- [Update Query Builder](/documentation/components/libs/postgresql/update-query-builder.md): UPDATE with FROM clause,
  RETURNING
- [Delete Query Builder](/documentation/components/libs/postgresql/delete-query-builder.md): DELETE with USING clause,
  RETURNING
- [Merge Query Builder](/documentation/components/libs/postgresql/merge-query-builder.md): MERGE (SQL:2008 upsert)

Query Building Utilities

- [Condition Builder](/documentation/components/libs/postgresql/condition-builder.md): build WHERE conditions incrementally with fluent API

Schema Management (DDL)

- [Table Query Builder](/documentation/components/libs/postgresql/table-query-builder.md): CREATE/ALTER/DROP TABLE
- [Index Query Builder](/documentation/components/libs/postgresql/index-query-builder.md): CREATE/DROP INDEX
- [View Query Builder](/documentation/components/libs/postgresql/view-query-builder.md): CREATE/DROP VIEW, materialized
  views
- [Sequence Query Builder](/documentation/components/libs/postgresql/sequence-query-builder.md): CREATE/ALTER/DROP
  SEQUENCE
- [Schema Query Builder](/documentation/components/libs/postgresql/schema-query-builder.md): CREATE/DROP SCHEMA

Database Administration

- [Transaction Query Builder](/documentation/components/libs/postgresql/transaction-query-builder.md): BEGIN, COMMIT,
  ROLLBACK, SAVEPOINT
- [Role & Grant Query Builder](/documentation/components/libs/postgresql/role-grant-query-builder.md): CREATE/ALTER
  ROLE, GRANT/REVOKE
- [Utility Query Builder](/documentation/components/libs/postgresql/utility-query-builder.md): VACUUM, ANALYZE,
  EXPLAIN, LOCK, CLUSTER

Extensions & Types

- [Copy Query Builder](/documentation/components/libs/postgresql/copy-query-builder.md): COPY for bulk data
  import/export
- [Trigger & Rule Query Builder](/documentation/components/libs/postgresql/trigger-rule-query-builder.md): CREATE/DROP
  TRIGGER, RULE
- [Function & Procedure Query Builder](/documentation/components/libs/postgresql/function-procedure-query-builder.md):
  CREATE/DROP FUNCTION, PROCEDURE
- [Extension Query Builder](/documentation/components/libs/postgresql/extension-query-builder.md): CREATE/DROP
  EXTENSION
- [Type Query Builder](/documentation/components/libs/postgresql/type-query-builder.md): CREATE/DROP TYPE (enum,
  composite, range)
- [Domain Query Builder](/documentation/components/libs/postgresql/domain-query-builder.md): CREATE/DROP DOMAIN

---

## Client

Execute queries against PostgreSQL with automatic type conversion and object mapping.

### Quick Start

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

// Connect
$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb user=postgres'));

// Fetch single row
$user = $client->fetch('SELECT * FROM users WHERE id = $1', [1]);

// Fetch all rows
$users = $client->fetchAll('SELECT * FROM users WHERE active = $1', [true]);

// Execute INSERT/UPDATE/DELETE
$affected = $client->execute('UPDATE users SET active = $1 WHERE id = $2', [false, 1]);

// The same, with parameters already in PostgreSQL text form - no converter runs
$affected = $client->execute('UPDATE users SET active = $1 WHERE id = $2', converted_parameters(['f', '1']));

// Transaction with automatic commit/rollback
$result = $client->transaction(function ($client) {
    $client->execute('INSERT INTO users (name) VALUES ($1)', ['John']);
    return $client->fetchScalar('SELECT lastval()');
});

// Close when done
$client->close();
```

### Array Types

Type-specific converters support PostgreSQL arrays. Use `typed()` with the correct array type:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection, typed, value_type_int4_array, value_type_text_array};

$client = pgsql_client(pgsql_connection('host=localhost dbname=mydb'));

// Integer array
$client->execute(
    'INSERT INTO scores (values) VALUES ($1)',
    [typed([100, 200, 300], value_type_int4_array())]
);

// Text array
$client->execute(
    'INSERT INTO tags (names) VALUES ($1)',
    [typed(['php', 'postgresql'], value_type_text_array())]
);
```

For all supported array types, read [Type System](/documentation/components/libs/postgresql/client-types.md).

### Row Mappers

A `RowMapper` turns a raw database row (an associative `array<string, mixed>`) into a typed result. You give mappers
to the `fetchInto`, `fetchOneInto`, `fetchAllInto`, and `Cursor::map()` methods. The client then returns objects,
value objects, or typed arrays instead of plain arrays.

The contract is one method:

```php
<?php

namespace Flow\PostgreSql\Client;

/**
 * @template T
 */
interface RowMapper
{
    /**
     * @param array<string, mixed> $row
     *
     * @throws Exception\MappingException
     *
     * @return T
     */
    public function map(array $row) : mixed;
}
```

The library has two default mappers. DSL functions give access to both. An optional bridge for
[cuyz/valinor](https://valinor.cuyz.io) is also available.

| Mapper                                                                                           | Use for                                                                                                                                                                                             |
|--------------------------------------------------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------|
| [ConstructorMapper](/documentation/components/libs/postgresql/client-constructor-mapper.md)      | Map row columns directly to constructor parameters by name (1:1). No type coercion.                                                                                                                 |
| [StaticFactoryMapper](/documentation/components/libs/postgresql/client-static-factory-mapper.md) | Delegate row → object construction to a public static factory method (`self::fromRow(array $row)`). Use it for a target class with a private constructor, or with custom coercion in the factory.   |
| [TypeMapper](/documentation/components/libs/postgresql/client-type-mapper.md)                    | Validate and coerce the row via [flow-php/types](/documentation/components/libs/types.md) (JSONB → structure, date string → `\DateTimeImmutable`, ...). Optionally chains into another `RowMapper`. |
| [PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md)      | Strict object hydration of complex graphs through cuyz/valinor. Needs the separate `flow-php/postgresql-valinor-bridge` package.                                                                    |

### Detailed Documentation

- [Connection](/documentation/components/libs/postgresql/client-connection.md): connection parameters, DSN parsing,
  lifecycle
- [Fetching Data](/documentation/components/libs/postgresql/client-fetching.md): fetch, fetchOne, fetchAll, fetchScalar
- [ConstructorMapper](/documentation/components/libs/postgresql/client-constructor-mapper.md): map rows directly to
  constructor parameters
- [StaticFactoryMapper](/documentation/components/libs/postgresql/client-static-factory-mapper.md): map rows via a
  public static factory method on the target class
- [TypeMapper](/documentation/components/libs/postgresql/client-type-mapper.md): validate and coerce rows through
  flow-php/types, and chain into another mapper
- [PostgreSQL Valinor Bridge](/documentation/components/bridges/postgresql-valinor-bridge.md): strict object
  hydration via cuyz/valinor *(optional dependency)*
- [Cursors](/documentation/components/libs/postgresql/client-cursor.md): memory-efficient streaming for large result
  sets
- [Transactions](/documentation/components/libs/postgresql/client-transactions.md): transaction callback pattern,
  nesting
- [Type System](/documentation/components/libs/postgresql/client-types.md): value converters, TypedValue, custom types
- [Query Plan Analysis](/documentation/components/libs/postgresql/client-explain.md): EXPLAIN ANALYZE, plan insights,
  performance debugging
- [Telemetry](/documentation/components/libs/postgresql/client-telemetry.md): openTelemetry tracing and metrics

---

## Query Modification

Modify existing SQL queries in code, for example to add pagination.

The pagination helpers and modifiers take exactly one read-only `SELECT` or `VALUES` statement. All other input
throws `InvalidStatementException`, for example `INSERT ... RETURNING`, two statements, a data-modifying `WITH` or
`SELECT ... INTO`.

### Offset Pagination

Add LIMIT/OFFSET pagination to any SELECT query:

```php
<?php

use function Flow\PostgreSql\DSL\sql_to_paginated_query;

$sql = 'SELECT * FROM users ORDER BY created_at DESC';

$page1 = sql_to_paginated_query($sql, limit: 25, offset: 0);
// SELECT * FROM users ORDER BY created_at DESC LIMIT 25

$page2 = sql_to_paginated_query($sql, limit: 25, offset: 25);
// SELECT * FROM users ORDER BY created_at DESC LIMIT 25 OFFSET 25
```

It works with complex queries, also with JOINs, CTEs, subqueries, and UNION:

```php
<?php

use function Flow\PostgreSql\DSL\sql_to_paginated_query;

$sql = <<<'SQL'
    WITH active_users AS (
        SELECT id, name FROM users WHERE status = 'active'
    )
    SELECT au.*, COUNT(o.id) as order_count
    FROM active_users au
    LEFT JOIN orders o ON au.id = o.user_id
    GROUP BY au.id, au.name
    ORDER BY order_count DESC
    SQL;

$paginated = sql_to_paginated_query($sql, limit: 10, offset: 0);
```

### Count Query Generation

Generate COUNT queries for pagination UIs ("Page 1 of 10"):

```php
<?php

use function Flow\PostgreSql\DSL\{sql_to_count_query, sql_to_paginated_query};

$sql = 'SELECT * FROM products WHERE active = true ORDER BY name';

$countQuery = sql_to_count_query($sql);
// SELECT count(*) FROM (SELECT * FROM products WHERE active = true) _count_subq

$page1 = sql_to_paginated_query($sql, limit: 20, offset: 0);
```

The COUNT modifier removes ORDER BY (an optimization) and wraps the query in a subquery.

### Keyset (Cursor) Pagination

For large datasets, keyset pagination is more efficient than OFFSET. It uses indexed WHERE conditions. It does not
scan and skip rows:

```php
<?php

use Flow\PostgreSql\AST\Transformers\SortOrder;

use function Flow\PostgreSql\DSL\{sql_to_keyset_query, sql_keyset_column};

$sql = 'SELECT * FROM audit_log ORDER BY created_at DESC, id DESC';

$columns = [
    sql_keyset_column('created_at', SortOrder::DESC),
    sql_keyset_column('id', SortOrder::DESC),
];

$page1 = sql_to_keyset_query($sql, limit: 100, columns: $columns, cursor: null);
// SELECT * FROM audit_log ORDER BY created_at DESC, id DESC LIMIT 100

$page2 = sql_to_keyset_query($sql, limit: 100, columns: $columns, cursor: ['2025-01-15 14:30:00', 1000]);
// SELECT * FROM audit_log WHERE (created_at, id) < ($1, $2) ORDER BY created_at DESC, id DESC LIMIT 100
```

The cursor values come from the last row of the previous page. Keyset pagination:

- Uses O(log n) index lookups instead of O(n) row scanning
- Uses a row comparison `(a, b) > ($1, $2)` for keys that all sort in the same direction, and an `OR` expansion for
  mixed directions
- Works with existing WHERE conditions (combined with AND)

Rules:

- Key columns must be non-null and unique together. A NULL cursor value throws `PaginationException`.
- ORDER BY is optional. An ORDER BY must list exactly the keys, in order, with no NULLS clause, ordinal,
  expression or USING.
- The query must not have its own LIMIT or OFFSET. Give the page size as `limit`.

---

## Advanced Features

### Custom AST Traversal

For advanced use cases, traverse the AST with custom visitors:

```php
<?php

use Flow\PostgreSql\AST\NodeVisitor;
use Flow\PostgreSql\Protobuf\AST\ColumnRef;

use function Flow\PostgreSql\DSL\sql_parse;

class ColumnCounter implements NodeVisitor
{
    public int $count = 0;

    public static function nodeClasses(): array
    {
        return [ColumnRef::class];
    }

    public function enter(object $node): ?int
    {
        $this->count++;
        return null;
    }

    public function leave(object $node): ?int
    {
        return null;
    }
}

$query = sql_parse('SELECT id, name, email FROM users');

$counter = new ColumnCounter();
$query->traverse($counter);

echo $counter->count; // 3
```

#### NodeVisitor Interface

```php
interface NodeVisitor
{
    public const DONT_TRAVERSE_CHILDREN = 1;
    public const STOP_TRAVERSAL = 2;

    /** @return list<class-string> */
    public static function nodeClasses(): array;

    public function enter(object $node): ?int;
    public function leave(object $node): ?int;
}
```

Visitors declare the node types that they handle with `nodeClasses()` (one or many). Return values:

- `null`: continue traversal
- `DONT_TRAVERSE_CHILDREN`: skip children (from `enter()` only)
- `STOP_TRAVERSAL`: stop entire traversal

#### Built-in Visitors

- `ColumnRefCollector`: collects all `ColumnRef` nodes
- `FuncCallCollector`: collects all `FuncCall` nodes
- `RangeVarCollector`: collects all `RangeVar` nodes
- `RelationCollector`: collects a `RangeVar` for each relation. It skips CTE references and `FOR UPDATE OF` names, and it builds one for `DROP` / `COMMENT ON` targets

### Custom Modifiers

To create a custom modifier, implement the `NodeModifier` interface:

```php
<?php

use Flow\PostgreSql\AST\{ModificationContext, NodeModifier};
use Flow\PostgreSql\Protobuf\AST\SelectStmt;

use function Flow\PostgreSql\DSL\{sql_parse, sql_deparse};

final readonly class AddDistinctModifier implements NodeModifier
{
    public static function nodeClasses(): array
    {
        return [SelectStmt::class];
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        if (!$context->isTopLevel()) {
            return null;
        }

        $node->setDistinctClause([new \Flow\PostgreSql\Protobuf\AST\Node()]);

        return null;
    }
}

$query = sql_parse('SELECT id, name FROM users');
$query->traverse(new AddDistinctModifier());
echo sql_deparse($query); // SELECT DISTINCT id, name FROM users
```

#### NodeModifier Interface

```php
interface NodeModifier
{
    /** @return list<class-string> */
    public static function nodeClasses(): array;

    public function modify(object $node, ModificationContext $context): int|object|null;
}
```

The `ModificationContext` has:

- `$context->depth()`: traversal depth: 1 for a top-level statement, +1 per message edge
- `$context->ancestors()`: parent messages from the root statement down, without `Node` wrappers
- `$context->parent()`: immediate parent message
- `$context->isTopLevel()`: whether this is the top-level statement

Return values:

- `null`: continue traversal
- `NodeModifier::DONT_TRAVERSE_CHILDREN`: skip children
- `NodeModifier::STOP_TRAVERSAL`: stop entire traversal
- `NodeModifier::REMOVE_NODE`: remove the node from its list (throws for a single-node slot)
- `object`: replace the current node

Replacement rules:

- A slot that holds a `Node` takes a `Node`. Each other slot takes the same class. Anything else throws.
- A replacement is not traversed (unlike PHP-Parser). To traverse it, run a second traversal.
- To clear a single slot, mutate the parent.

### Using Modifiers Directly

For more control, use modifier objects directly with `traverse()`:

```php
<?php

use Flow\PostgreSql\AST\Transformers\{CountModifier, KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, OffsetPaginationConfig, OffsetPaginationModifier, SortOrder};

use function Flow\PostgreSql\DSL\sql_parse;

// Offset pagination modifier
$query = sql_parse('SELECT * FROM users ORDER BY id');
$query->traverse(new OffsetPaginationModifier(new OffsetPaginationConfig(limit: 10, offset: 20)));
echo $query->deparse(); // SELECT * FROM users ORDER BY id LIMIT 10 OFFSET 20

// Count modifier
$query = sql_parse('SELECT * FROM users WHERE active = true ORDER BY name');
$query->traverse(new CountModifier());
echo $query->deparse(); // SELECT count(*) FROM (SELECT * FROM users WHERE active = true) _count_subq

// Keyset pagination modifier
$query = sql_parse('SELECT * FROM users ORDER BY created_at, id');
$query->traverse(new KeysetPaginationModifier(new KeysetPaginationConfig(
    limit: 10,
    columns: [
        new KeysetColumn('created_at', SortOrder::ASC),
        new KeysetColumn('id', SortOrder::ASC),
    ],
    cursor: ['2025-01-15', 42]
)));
echo $query->deparse();
// SELECT * FROM users WHERE (created_at, id) > ($1, $2) ORDER BY created_at, id LIMIT 10
```

`ExplainModifier` wraps each statement before the traversal visits any of them. Run it in its own `traverse()` after
the other modifiers:

```php
<?php

use Flow\PostgreSql\AST\Transformers\{ExplainConfig, ExplainModifier, OffsetPaginationConfig, OffsetPaginationModifier};

use function Flow\PostgreSql\DSL\sql_parse;

$query = sql_parse('SELECT * FROM users ORDER BY id');
$query->traverse(new OffsetPaginationModifier(new OffsetPaginationConfig(limit: 10)));
$query->traverse(new ExplainModifier(ExplainConfig::forEstimate()));
echo $query->deparse(); // EXPLAIN (COSTS 1, FORMAT "json") SELECT * FROM users ORDER BY id LIMIT 10
```

Give `param()` instead of a number to leave the value to the parameters of the query. Each page then sends the same SQL:

```php
<?php

use Flow\PostgreSql\AST\Transformers\{OffsetPaginationConfig, OffsetPaginationModifier};

use function Flow\PostgreSql\DSL\{param, sql_parse};

$query = sql_parse('SELECT * FROM users WHERE active = $1 ORDER BY id');
$query->traverse(new OffsetPaginationModifier(new OffsetPaginationConfig(limit: param(2), offset: param(3))));
echo $query->deparse(); // SELECT * FROM users WHERE active = $1 ORDER BY id LIMIT $2 OFFSET $3
```

### Raw AST Access

For full control, access the protobuf AST directly:

```php
<?php

use function Flow\PostgreSql\DSL\sql_parse;

$query = sql_parse('SELECT id FROM users WHERE active = true');

foreach ($query->raw()->getStmts() as $stmt) {
    $select = $stmt->getStmt()->getSelectStmt();

    // Access FROM clause
    foreach ($select->getFromClause() as $from) {
        echo $from->getRangeVar()->getRelname();
    }

    // Access WHERE clause
    $where = $select->getWhereClause();
    // ...
}
```

---

## Schema Diff

Compare two catalogs and generate the SQL that changes the source into the target.

```php
<?php

use Flow\PostgreSql\Schema\Catalog;

use function Flow\PostgreSql\DSL\{catalog_comparator, schema, schema_column_integer, schema_table};

$current = new Catalog([schema('public', tables: [schema_table('users', [schema_column_integer('id', false)])])]);
$target = new Catalog([schema('public')]); // users removed

foreach (catalog_comparator()->compare($current, $target)->generate() as $sql) {
    echo $sql->toSql() . "\n"; // DROP TABLE public.users CASCADE
}
```

### `dropIfExists`

By default, generated `DROP` statements have no `IF EXISTS`, so a missing object fails with an error (drift
detection). Set `dropIfExists: true` to render each generated drop with `IF EXISTS`. This helps convergence migrations
that must run against new and legacy databases:

```php
// On the comparator (applies to every compare):
catalog_comparator(dropIfExists: true)->compare($current, $target);

// Or per call (overrides the comparator's default):
catalog_comparator()->compare($current, $target, dropIfExists: true);
// -> DROP TABLE IF EXISTS public.users CASCADE
```

---

## Reference

### Exception Handling

```php
<?php

use Flow\PostgreSql\AST\Transformers\{OffsetPaginationConfig, OffsetPaginationModifier};
use Flow\PostgreSql\Exception\{ParserException, ExtensionNotLoadedException, PaginationException};

use function Flow\PostgreSql\DSL\sql_parse;

try {
    $query = sql_parse('INVALID SQL');
} catch (ExtensionNotLoadedException $e) {
    // pg_query extension is not loaded
} catch (ParserException $e) {
    echo "Parse error: " . $e->getMessage();
}

try {
    // OFFSET without ORDER BY throws exception
    $query = sql_parse('SELECT * FROM users');
    $query->traverse(new OffsetPaginationModifier(new OffsetPaginationConfig(limit: 10, offset: 5)));
} catch (PaginationException $e) {
    echo "Pagination error: " . $e->getMessage();
    // "OFFSET without ORDER BY produces non-deterministic results"
}
```

### Performance

For the best protobuf parse performance, install the `ext-protobuf` PHP extension:

```bash
pecl install protobuf
```

Without it, the library uses the pure PHP implementation from `google/protobuf`. The native extension is much
faster.
