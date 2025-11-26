# PostgreSQL Query Parser

Flow PHP's PostgreSQL Query Parser library provides strongly-typed AST (Abstract Syntax Tree) parsing
for PostgreSQL SQL queries using the [libpg_query](https://github.com/pganalyze/libpg_query) library
through a PHP extension.

## Installation

This library requires the `pg_query` PHP extension. Install it via PIE:

```bash
pie install flow-php/pg-query-ext
```

Then install the library via Composer:

```bash
composer require flow-php/pg-query
```

## Usage

```php
<?php

use Flow\PgQuery\Parser;
use function Flow\PgQuery\DSL\pg_parse;

// Using the Parser class directly
$parser = new Parser();
$result = $parser->parse('SELECT id, name FROM users WHERE active = true');

// Access the AST
foreach ($result->getStmts() as $stmt) {
    $node = $stmt->getStmt();
    // Work with the parsed statement...
}

// Or use DSL functions for convenience
$result = pg_parse('SELECT 1');

// Other utilities
$fingerprint = $parser->fingerprint('SELECT id FROM users WHERE id = 1');
$normalized = $parser->normalize('SELECT * FROM users WHERE id = 1'); // Returns: SELECT * FROM users WHERE id = $1
$statements = $parser->split('SELECT 1; SELECT 2;'); // Returns: ['SELECT 1', 'SELECT 2']
```

## Features

- **Full PostgreSQL SQL parsing** - Uses the actual PostgreSQL parser
- **Strongly-typed AST nodes** - Generated from protobuf definitions
- **Query fingerprinting** - Generate unique fingerprints for queries
- **Query normalization** - Replace literals with parameter placeholders
- **Statement splitting** - Split multiple SQL statements

## Documentation

- [Official Documentation](https://flow-php.com)
- [GitHub Repository](https://github.com/flow-php/flow)
