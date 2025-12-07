# PG Query

- [⬅️️ Back](/documentation/introduction.md)
- [📚API Reference](/documentation/api/lib/pg-query)
- [📁Files](/documentation/api/lib/pg-query/indices/files.html)
- [🗺DSL](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html)

[TOC]

PostgreSQL Query Parser library provides strongly-typed AST (Abstract Syntax Tree) parsing for PostgreSQL SQL queries using the [libpg_query](https://github.com/pganalyze/libpg_query) library through a PHP extension.

## Requirements

This library requires the `pg_query` PHP extension. See [pg-query-ext documentation](/documentation/components/extensions/pg-query-ext.md) for installation instructions.

## Installation

```
composer require flow-php/pg-query:~--FLOW_PHP_VERSION--
```

## Quick Start

```php
<?php

use function Flow\PgQuery\DSL\{sql_parse, sql_query_tables, sql_query_columns, sql_query_functions};

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

## Parsing and Utilities

```php
<?php

use function Flow\PgQuery\DSL\{
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

## Deparsing (AST to SQL)

Convert a parsed query back to SQL, optionally with pretty-printing:

```php
<?php

use function Flow\PgQuery\DSL\{sql_parse, sql_deparse, sql_deparse_options, sql_format};

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

### DeparseOptions

| Method | Description | Default |
|--------|-------------|---------|
| `prettyPrint(bool)` | Enable/disable pretty printing | `true` |
| `indentSize(int)` | Spaces per indentation level | `4` |
| `maxLineLength(int)` | Maximum line length before wrapping | `80` |
| `trailingNewline(bool)` | Add trailing newline at end | `false` |
| `commasStartOfLine(bool)` | Place commas at start of lines | `false` |

## Custom AST Traversal

For advanced use cases, you can traverse the AST with custom visitors:

```php
<?php

use Flow\PgQuery\AST\NodeVisitor;
use Flow\PgQuery\Protobuf\AST\ColumnRef;

use function Flow\PgQuery\DSL\sql_parse;

class ColumnCounter implements NodeVisitor
{
    public int $count = 0;

    public static function nodeClass(): string
    {
        return ColumnRef::class;
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

### NodeVisitor Interface

```php
interface NodeVisitor
{
    public const DONT_TRAVERSE_CHILDREN = 1;
    public const STOP_TRAVERSAL = 2;

    /** @return class-string */
    public static function nodeClass(): string;

    public function enter(object $node): ?int;
    public function leave(object $node): ?int;
}
```

Visitors declare which node type they handle via `nodeClass()`. Return values:
- `null` - continue traversal
- `DONT_TRAVERSE_CHILDREN` - skip children (from `enter()` only)
- `STOP_TRAVERSAL` - stop entire traversal

### Built-in Visitors

- `ColumnRefCollector` - collects all `ColumnRef` nodes
- `FuncCallCollector` - collects all `FuncCall` nodes
- `RangeVarCollector` - collects all `RangeVar` nodes

## Query Modification

Beyond reading the AST, you can modify queries programmatically using modifiers. The library includes pagination modifiers as the primary use case.

### Offset-Based Pagination

Add LIMIT/OFFSET pagination to any SELECT query:

```php
<?php

use function Flow\PgQuery\DSL\sql_to_paginated_query;

$sql = 'SELECT * FROM users ORDER BY created_at DESC';

$page1 = sql_to_paginated_query($sql, limit: 25, offset: 0);
// SELECT * FROM users ORDER BY created_at DESC LIMIT 25

$page2 = sql_to_paginated_query($sql, limit: 25, offset: 25);
// SELECT * FROM users ORDER BY created_at DESC LIMIT 25 OFFSET 25
```

Works with complex queries including JOINs, CTEs, subqueries, and UNION:

```php
<?php

use function Flow\PgQuery\DSL\sql_to_paginated_query;

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

use function Flow\PgQuery\DSL\{sql_to_count_query, sql_to_paginated_query};

$sql = 'SELECT * FROM products WHERE active = true ORDER BY name';

$countQuery = sql_to_count_query($sql);
// SELECT count(*) FROM (SELECT * FROM products WHERE active = true) _count_subq

$page1 = sql_to_paginated_query($sql, limit: 20, offset: 0);
```

The COUNT modifier automatically removes ORDER BY (optimization) and wraps the query in a subquery.

### Keyset (Cursor) Pagination

For large datasets, keyset pagination is more efficient than OFFSET. It uses indexed WHERE conditions instead of scanning and skipping rows:

```php
<?php

use Flow\PgQuery\AST\Transformers\SortOrder;

use function Flow\PgQuery\DSL\{sql_to_keyset_query, sql_keyset_column};

$sql = 'SELECT * FROM audit_log ORDER BY created_at DESC, id DESC';

$columns = [
    sql_keyset_column('created_at', SortOrder::DESC),
    sql_keyset_column('id', SortOrder::DESC),
];

$page1 = sql_to_keyset_query($sql, limit: 100, columns: $columns, cursor: null);
// SELECT * FROM audit_log ORDER BY created_at DESC, id DESC LIMIT 100

$page2 = sql_to_keyset_query($sql, limit: 100, columns: $columns, cursor: ['2025-01-15 14:30:00', 1000]);
// SELECT * FROM audit_log WHERE created_at < $1 OR (created_at = $1 AND id < $2) ORDER BY created_at DESC, id DESC LIMIT 100
```

The cursor values come from the last row of the previous page. Keyset pagination:
- Uses O(log n) index lookups instead of O(n) row scanning
- Handles mixed ASC/DESC sort orders correctly
- Works with existing WHERE conditions (combined with AND)

### Using Modifiers Directly

For more control, you can use modifier objects directly with `traverse()`:

```php
<?php

use Flow\PgQuery\AST\Transformers\{CountModifier, KeysetColumn, KeysetPaginationConfig, KeysetPaginationModifier, PaginationConfig, PaginationModifier, SortOrder};

use function Flow\PgQuery\DSL\sql_parse;

// Offset pagination modifier
$query = sql_parse('SELECT * FROM users ORDER BY id');
$query->traverse(new PaginationModifier(new PaginationConfig(limit: 10, offset: 20)));
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
// SELECT * FROM users WHERE created_at > $1 OR (created_at = $1 AND id > $2) ORDER BY created_at, id LIMIT 10
```

### Custom Modifiers

Create custom modifiers by implementing the `NodeModifier` interface:

```php
<?php

use Flow\PgQuery\AST\{ModificationContext, NodeModifier};
use Flow\PgQuery\Protobuf\AST\SelectStmt;

use function Flow\PgQuery\DSL\{sql_parse, sql_deparse};

final readonly class AddDistinctModifier implements NodeModifier
{
    public static function nodeClass(): string
    {
        return SelectStmt::class;
    }

    public function modify(object $node, ModificationContext $context): int|object|null
    {
        if (!$context->isTopLevel()) {
            return null;
        }

        $node->setDistinctClause([new \Flow\PgQuery\Protobuf\AST\Node()]);

        return null;
    }
}

$query = sql_parse('SELECT id, name FROM users');
$query->traverse(new AddDistinctModifier());
echo sql_deparse($query); // SELECT DISTINCT id, name FROM users
```

### NodeModifier Interface

```php
interface NodeModifier
{
    /** @return class-string */
    public static function nodeClass(): string;

    public function modify(object $node, ModificationContext $context): int|object|null;
}
```

The `ModificationContext` provides:
- `$context->depth` - current traversal depth
- `$context->ancestors` - array of parent nodes
- `$context->getParent()` - immediate parent node
- `$context->isTopLevel()` - whether this is the top-level statement

Return values:
- `null` - continue traversal
- `Traverser::DONT_TRAVERSE_CHILDREN` - skip children
- `Traverser::STOP_TRAVERSAL` - stop entire traversal
- `object` - replace current node with returned object

## Query Builder

The library also provides a fluent, type-safe query builder for constructing PostgreSQL queries programmatically. Instead of string concatenation, you build queries using a step-by-step builder pattern that guides you through valid SQL construction.

For complete documentation on each query builder type, see:

- [Select Query Builder](pg-query/select-query-builder.md)
- [Insert Query Builder](pg-query/insert-query-builder.md)
- [Update Query Builder](pg-query/update-query-builder.md)
- [Delete Query Builder](pg-query/delete-query-builder.md)
- [Merge Query Builder](pg-query/merge-query-builder.md)
- [Copy Query Builder](pg-query/copy-query-builder.md)
- [Transaction Query Builder](pg-query/transaction-query-builder.md)
- [Table Query Builder](pg-query/table-query-builder.md)
- [Index Query Builder](pg-query/index-query-builder.md)
- [Utility Query Builder](pg-query/utility-query-builder.md)


```php
<?php

use function Flow\PgQuery\DSL\{
    select, col, table, literal_int, eq, asc
};

// Build a SELECT query
$query = select()
    ->select(col('id'), col('name'), col('email'))
    ->from(table('users'))
    ->where(eq(col('active'), literal_int(1)))
    ->orderBy(asc(col('name')))
    ->limit(10);

// Convert to SQL string
echo $query->toSQL();
// SELECT id, name, email FROM users WHERE active = 1 ORDER BY name LIMIT 10
```

```php
<?php

use function Flow\PgQuery\DSL\{
    insert, col_from_string, param, conflict_columns, returning_all
};

// Build an upsert query
$query = insert()
    ->into('users')
    ->columns('email', 'name')
    ->values(param(1), param(2))
    ->onConflictDoUpdate(
        conflict_columns(['email']),
        ['name' => col_from_string('excluded.name')]
    )
    ->returningAll();

echo $query->toSQL();
// INSERT INTO users (email, name) VALUES ($1, $2)
// ON CONFLICT (email) DO UPDATE SET name = excluded.name RETURNING *
```

## Raw AST Access

For full control, access the protobuf AST directly:

```php
<?php

use function Flow\PgQuery\DSL\sql_parse;

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

## Exception Handling

```php
<?php

use Flow\PgQuery\AST\Transformers\{PaginationConfig, PaginationModifier};
use Flow\PgQuery\Exception\{ParserException, ExtensionNotLoadedException, PaginationException};

use function Flow\PgQuery\DSL\sql_parse;

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
    $query->traverse(new PaginationModifier(new PaginationConfig(limit: 10, offset: 5)));
} catch (PaginationException $e) {
    echo "Pagination error: " . $e->getMessage();
    // "OFFSET without ORDER BY produces non-deterministic results"
}
```

## Performance

For optimal protobuf parsing performance, install the `ext-protobuf` PHP extension:

```bash
pecl install protobuf
```

The library works without it using the pure PHP implementation from `google/protobuf`, but the native extension provides significantly better performance.
