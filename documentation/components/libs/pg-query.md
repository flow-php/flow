# PG Query

- [⬅️️ Back](/documentation/introduction.md)

PostgreSQL Query Parser library provides strongly-typed AST (Abstract Syntax Tree) parsing for PostgreSQL SQL queries using the [libpg_query](https://github.com/pganalyze/libpg_query) library through a PHP extension.

This library wraps the low-level extension functions and provides:
- Strongly-typed AST nodes generated from protobuf definitions
- A `Parser` class for object-oriented access
- DSL helper functions for convenient usage

## Requirements

This library requires the `pg_query` PHP extension. See [pg-query-ext documentation](/documentation/components/extensions/pg-query-ext.md) for installation instructions.

## Installation

```
composer require flow-php/pg-query:~--FLOW_PHP_VERSION--
```

## Usage

### Using the Parser Class

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();

// Parse SQL into AST
$result = $parser->parse('SELECT id, name FROM users WHERE active = true');

// Access the AST
foreach ($result->getStmts() as $stmt) {
    $node = $stmt->getStmt();
    $selectStmt = $node->getSelectStmt();
    // Work with strongly-typed AST nodes...
}
```

### Using DSL Functions

```php
<?php

use function Flow\PgQuery\DSL\pg_parse;
use function Flow\PgQuery\DSL\pg_parser;
use function Flow\PgQuery\DSL\pg_fingerprint;
use function Flow\PgQuery\DSL\pg_normalize;
use function Flow\PgQuery\DSL\pg_split;

// Parse SQL
$result = pg_parse('SELECT * FROM users');

// Get a reusable parser instance
$parser = pg_parser();

// Generate fingerprint
$fingerprint = pg_fingerprint('SELECT id FROM users WHERE id = 1');

// Normalize query
$normalized = pg_normalize('SELECT * FROM users WHERE id = 1');

// Split multiple statements
$statements = pg_split('SELECT 1; SELECT 2;');
```

## Features

### Query Parsing

Parse PostgreSQL SQL into a strongly-typed AST:

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();
$result = $parser->parse('SELECT id, name FROM users WHERE active = true ORDER BY name');

foreach ($result->getStmts() as $stmt) {
    $selectStmt = $stmt->getStmt()->getSelectStmt();

    // Access FROM clause
    foreach ($selectStmt->getFromClause() as $fromItem) {
        $rangeVar = $fromItem->getRangeVar();
        echo "Table: " . $rangeVar->getRelname() . "\n";
    }

    // Access target list (SELECT columns)
    foreach ($selectStmt->getTargetList() as $target) {
        $columnRef = $target->getResTarget()->getVal()->getColumnRef();
        // Process column references...
    }
}
```

### Query Fingerprinting

Generate unique fingerprints for structurally equivalent queries. This is useful for grouping similar queries regardless of their literal values:

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();

// These queries produce the same fingerprint
$fp1 = $parser->fingerprint('SELECT * FROM users WHERE id = 1');
$fp2 = $parser->fingerprint('SELECT * FROM users WHERE id = 999');

var_dump($fp1 === $fp2); // true
```

### Query Normalization

Replace literal values with parameter placeholders:

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();

$normalized = $parser->normalize("SELECT * FROM users WHERE name = 'John' AND age = 25");
// Returns: SELECT * FROM users WHERE name = $1 AND age = $2
```

### Statement Splitting

Split a string containing multiple SQL statements:

```php
<?php

use Flow\PgQuery\Parser;

$parser = new Parser();

$statements = $parser->split('SELECT 1; SELECT 2; SELECT 3');
// Returns: ['SELECT 1', ' SELECT 2', ' SELECT 3']
```

## API Reference

### Parser Class

| Method | Description | Returns |
|--------|-------------|---------|
| `parse(string $sql)` | Parse SQL into AST | `ParseResult` |
| `fingerprint(string $sql)` | Generate query fingerprint | `?string` |
| `normalize(string $sql)` | Normalize query with placeholders | `?string` |
| `split(string $sql)` | Split multiple statements | `array<string>` |

### DSL Functions

| Function | Description | Returns |
|----------|-------------|---------|
| `pg_parser()` | Create a new Parser instance | `Parser` |
| `pg_parse(string $sql)` | Parse SQL into AST | `ParseResult` |
| `pg_fingerprint(string $sql)` | Generate query fingerprint | `?string` |
| `pg_normalize(string $sql)` | Normalize query | `?string` |
| `pg_split(string $sql)` | Split statements | `array<string>` |

## AST Node Types

The library includes 343 strongly-typed AST node classes generated from PostgreSQL's protobuf definitions. All classes are in the `Flow\PgQuery\Protobuf\AST` namespace.

Common node types include:
- `SelectStmt` - SELECT statement
- `InsertStmt` - INSERT statement
- `UpdateStmt` - UPDATE statement
- `DeleteStmt` - DELETE statement
- `ColumnRef` - Column reference
- `A_Expr` - Expression node
- `FuncCall` - Function call
- `JoinExpr` - JOIN expression
- `RangeVar` - Table/view reference

## Exception Handling

```php
<?php

use Flow\PgQuery\Parser;
use Flow\PgQuery\Exception\ParserException;
use Flow\PgQuery\Exception\ExtensionNotLoadedException;

try {
    $parser = new Parser();
} catch (ExtensionNotLoadedException $e) {
    // pg_query extension is not loaded
}

try {
    $result = $parser->parse('INVALID SQL SYNTAX HERE');
} catch (ParserException $e) {
    echo "Parse error: " . $e->getMessage();
}
```

## Performance

For optimal protobuf parsing performance, install the `ext-protobuf` PHP extension:

```bash
pecl install protobuf
```

The library will work without it using the pure PHP implementation from `google/protobuf`, but the native extension provides significantly better performance for AST deserialization.
