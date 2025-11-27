# PG Query

- [⬅️️ Back](/documentation/introduction.md)

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

use function Flow\PgQuery\DSL\pg_parse;

$query = pg_parse('SELECT u.id, u.name FROM users u JOIN orders o ON u.id = o.user_id');

// Get all tables
foreach ($query->tables() as $table) {
    echo $table->name();  // 'users', 'orders'
    echo $table->alias(); // 'u', 'o'
}

// Get all columns
foreach ($query->columns() as $column) {
    echo $column->name();  // 'id', 'name', 'id', 'user_id'
    echo $column->table(); // 'u', 'u', 'u', 'o'
}

// Get columns for specific table
$userColumns = $query->columns('u');

// Get all function calls
foreach ($query->functions() as $func) {
    echo $func->name();   // function name
    echo $func->schema(); // schema if qualified (e.g., 'pg_catalog')
}
```

## Parser Class

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();

// Parse SQL into ParsedQuery
$query = $parser->parse('SELECT * FROM users WHERE id = 1');

// Generate fingerprint (same for structurally equivalent queries)
$fingerprint = $parser->fingerprint('SELECT * FROM users WHERE id = 1');

// Normalize query (replace literals with positional parameters)
$normalized = $parser->normalize("SELECT * FROM users WHERE name = 'John'");
// Returns: SELECT * FROM users WHERE name = $1

// Normalize also handles Doctrine-style named parameters
$normalized = $parser->normalize('SELECT * FROM users WHERE id = :id');
// Returns: SELECT * FROM users WHERE id = $1

// Split multiple statements
$statements = $parser->split('SELECT 1; SELECT 2;');
// Returns: ['SELECT 1', ' SELECT 2']
```

## DSL Functions

```php
<?php

use function Flow\PgQuery\DSL\{pg_parse, pg_parser, pg_fingerprint, pg_normalize, pg_split};

$query = pg_parse('SELECT * FROM users');
$parser = pg_parser();
$fingerprint = pg_fingerprint('SELECT * FROM users WHERE id = 1');
$normalized = pg_normalize('SELECT * FROM users WHERE id = 1');
$statements = pg_split('SELECT 1; SELECT 2;');
```

## ParsedQuery Methods

| Method | Description | Returns |
|--------|-------------|---------|
| `tables()` | Get all tables referenced in the query | `array<Table>` |
| `columns(?string $tableName)` | Get columns, optionally filtered by table/alias | `array<Column>` |
| `functions()` | Get all function calls | `array<FunctionCall>` |
| `traverse(NodeVisitor ...$visitors)` | Traverse AST with custom visitors | `void` |
| `raw()` | Access underlying protobuf ParseResult | `ParseResult` |

## Custom AST Traversal

For advanced use cases, you can traverse the AST with custom visitors:

```php
<?php

use Flow\PgQuery\AST\NodeVisitor;
use Flow\PgQuery\Protobuf\AST\ColumnRef;

use function Flow\PgQuery\DSL\pg_parse;

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

$query = pg_parse('SELECT id, name, email FROM users');

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

## Raw AST Access

For full control, access the protobuf AST directly:

```php
<?php

use function Flow\PgQuery\DSL\pg_parse;

$query = pg_parse('SELECT id FROM users WHERE active = true');

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

use Flow\PgQuery\Parser;
use Flow\PgQuery\Exception\{ParserException, ExtensionNotLoadedException};

try {
    $parser = new Parser();
} catch (ExtensionNotLoadedException $e) {
    // pg_query extension is not loaded
}

try {
    $parser->parse('INVALID SQL');
} catch (ParserException $e) {
    echo "Parse error: " . $e->getMessage();
}
```

## Performance

For optimal protobuf parsing performance, install the `ext-protobuf` PHP extension:

```bash
pecl install protobuf
```

The library works without it using the pure PHP implementation from `google/protobuf`, but the native extension provides significantly better performance.
