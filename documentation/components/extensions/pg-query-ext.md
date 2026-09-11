---
package: flow-php/pg-query-ext
---

# PG Query Extension

[PACKAGE_NAV]

[TOC]

A compiled PHP extension for PostgreSQL query parsing using [libpg_query](https://github.com/pganalyze/libpg_query).

This extension provides low-level functions for parsing PostgreSQL SQL queries. For a higher-level, object-oriented
interface with strongly-typed AST nodes, see the [PostgreSQL library](/documentation/components/libs/postgresql.md).

## Features

- Parse PostgreSQL SQL queries into JSON or protobuf AST
- Generate query fingerprints for query grouping
- Normalize SQL queries (replace literals with placeholders)
- Deparse AST back to SQL with optional pretty-printing
- Parse PL/pgSQL functions
- Split multiple SQL statements
- Scan SQL into tokens
- Generate query summaries for logging/monitoring
- Check if queries contain utility/DDL statements (without full parsing)

## Requirements

- PHP 8.2+
- C compiler (gcc/clang)
- git (for auto-downloading libpg_query)
- make
- protobuf-c library

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/pg-query-ext.md).

## Loading the Extension

### In php.ini

```ini
extension = pg_query
```

### During Development

```bash
php -d extension=/path/to/pg_query.so your_script.php
```

## Usage

```php
<?php

// Parse SQL and return JSON AST
$json = pg_query_parse('SELECT * FROM users WHERE id = 1');
$ast = json_decode($json, true);

// Parse SQL and return protobuf AST (more efficient for programmatic use)
$protobuf = pg_query_parse_protobuf('SELECT * FROM users WHERE id = 1');

// Generate fingerprint (same for structurally equivalent queries)
$fp = pg_query_fingerprint('SELECT * FROM users WHERE id = 1');
// Returns same fingerprint for: SELECT * FROM users WHERE id = 2

// Normalize query (replace literals with $N placeholders)
$normalized = pg_query_normalize("SELECT * FROM users WHERE name = 'John'");
// Returns: SELECT * FROM users WHERE name = $1

// Normalize utility/DDL statements
$normalized = pg_query_normalize_utility('CREATE TABLE users (id INT, name VARCHAR(255))');

// Split multiple statements
$statements = pg_query_split('SELECT 1; SELECT 2; SELECT 3');
// Returns: ['SELECT 1', ' SELECT 2', ' SELECT 3']

// Parse PL/pgSQL function
$plpgsql = pg_query_parse_plpgsql('
    CREATE FUNCTION add(a int, b int) RETURNS int AS $$
    BEGIN
        RETURN a + b;
    END;
    $$ LANGUAGE plpgsql;
');

// Scan SQL into tokens (returns protobuf data)
$tokens = pg_query_scan('SELECT 1');

// Deparse protobuf AST back to SQL
$protobuf = pg_query_parse_protobuf('SELECT id, name FROM users WHERE active = true');
$sql = pg_query_deparse($protobuf);
// Returns: SELECT id, name FROM users WHERE active = true

// Deparse with pretty-printing options
$sql = pg_query_deparse_opts(
    $protobuf,
    true,   // pretty_print
    4,      // indent_size
    80,     // max_line_length
    false,  // trailing_newline
    false   // commas_start_of_line
);
// Returns:
// SELECT id, name
// FROM users
// WHERE active = true

// Generate query summary (protobuf format, useful for logging)
$summary = pg_query_summary('SELECT * FROM users WHERE id = 1');

// Check if query contains utility statements (DDL) - fast, without full parsing
$isUtility = pg_query_is_utility_stmt('CREATE TABLE users (id int)');
// Returns: true

$isUtility = pg_query_is_utility_stmt('SELECT * FROM users');
// Returns: false
```

## Functions Reference

| Function                                                     | Description                                    | Returns             |
|--------------------------------------------------------------|------------------------------------------------|---------------------|
| `pg_query_parse(string $sql)`                                | Parse SQL to JSON AST                          | `string` (JSON)     |
| `pg_query_parse_protobuf(string $sql)`                       | Parse SQL to protobuf AST                      | `string` (protobuf) |
| `pg_query_fingerprint(string $sql)`                          | Generate query fingerprint                     | `string\|false`     |
| `pg_query_normalize(string $sql)`                            | Normalize query with placeholders              | `string\|false`     |
| `pg_query_normalize_utility(string $sql)`                    | Normalize DDL/utility statements               | `string\|false`     |
| `pg_query_parse_plpgsql(string $sql)`                        | Parse PL/pgSQL function                        | `string` (JSON)     |
| `pg_query_split(string $sql)`                                | Split multiple statements                      | `array<string>`     |
| `pg_query_scan(string $sql)`                                 | Scan SQL into tokens                           | `string` (protobuf) |
| `pg_query_deparse(string $protobuf)`                         | Convert protobuf AST back to SQL               | `string`            |
| `pg_query_deparse_opts(...)`                                 | Deparse with formatting options                | `string`            |
| `pg_query_summary(string $sql, int $options, int $truncate)` | Generate query summary                         | `string` (protobuf) |
| `pg_query_is_utility_stmt(string $sql)`                      | Check if query contains utility/DDL statements | `bool`              |

### pg_query_deparse_opts Parameters

```php ignore
pg_query_deparse_opts(
    string $protobuf,           // Protobuf AST from pg_query_parse_protobuf()
    bool $pretty_print = false, // Enable pretty printing
    int $indent_size = 4,       // Spaces per indentation level
    int $max_line_length = 80,  // Maximum line length before wrapping
    bool $trailing_newline = false,    // Add trailing newline
    bool $commas_start_of_line = false // Place commas at line start
): string
```

## Error Handling

The extension throws `RuntimeException` on parse errors:

```php
<?php

try {
    $result = pg_query_parse('INVALID SQL SYNTAX');
} catch (RuntimeException $e) {
    echo "Parse error: " . $e->getMessage();
}
```

## Development

### Build Commands

```bash
# Build and run tests
make test

# Build only
make build

# Rebuild extension only (without rebuilding libpg_query)
make rebuild

# Clean build artifacts
make clean

# Remove everything including libpg_query
make distclean
```

### Modifying the Extension

When modifying the C source files:

```bash
# Inside nix-shell with --arg with-pg-query-ext true
cd src/extension/pg-query-ext
make rebuild

# Test your changes
make test
```

## Architecture

The extension is built on top of [libpg_query](https://github.com/pganalyze/libpg_query), which extracts PostgreSQL's
query parser into a standalone library. This means you get the exact same SQL parsing behavior as PostgreSQL itself.

Key implementation details:

- **Static linking**: libpg_query.a is statically linked into the extension
- **Build dependency**: Requires `protobuf-c` library for compilation (libpg_query uses protobuf internally)
- **Auto-download**: The build system automatically downloads the correct libpg_query version

## See Also

- [PostgreSQL library](/documentation/components/libs/postgresql.md) - Higher-level PHP wrapper with strongly-typed AST
  nodes
- [libpg_query](https://github.com/pganalyze/libpg_query) - The underlying C library
- [Nix Development Environment](/documentation/contributing/nix.md) - Using nix-shell for development
