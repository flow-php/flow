# PG Query Extension

- [⬅️️ Back](/documentation/introduction.md)

A compiled PHP extension for PostgreSQL query parsing using [libpg_query](https://github.com/pganalyze/libpg_query).

This extension provides low-level functions for parsing PostgreSQL SQL queries. For a higher-level, object-oriented interface with strongly-typed AST nodes, see the [pg-query library](/documentation/components/libs/pg-query.md).

## Features

- Parse PostgreSQL SQL queries into JSON AST
- Generate query fingerprints for query grouping
- Normalize SQL queries (replace literals with placeholders)
- Parse PL/pgSQL functions
- Split multiple SQL statements
- Scan SQL into tokens

## Requirements

- PHP 8.2+
- C compiler (gcc/clang)
- git (for auto-downloading libpg_query)
- make
- protobuf-c library

## Installation

### Using PIE (Recommended)

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

```bash
# Simple installation (auto-downloads libpg_query for PostgreSQL 17)
pie install flow-php/pg-query-ext

# Install with a specific PostgreSQL grammar version (15, 16, or 17)
pie install flow-php/pg-query-ext --with-pg-version=16
```

The extension will automatically download and build the appropriate libpg_query version. Build dependencies (`protobuf-c`, `git`, `make`, `gcc`) must be available on your system.

### Supported PostgreSQL Versions

| PostgreSQL | libpg_query version |
|------------|---------------------|
| 17         | 17-6.1.0 (default)  |
| 16         | 16-5.2.0            |
| 15         | 15-4.2.4            |

## Loading the Extension

### In php.ini

```ini
extension=pg_query
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

// Generate fingerprint (same for structurally equivalent queries)
$fp = pg_query_fingerprint('SELECT * FROM users WHERE id = 1');
// Returns same fingerprint for: SELECT * FROM users WHERE id = 2

// Normalize query (replace literals with $N placeholders)
$normalized = pg_query_normalize("SELECT * FROM users WHERE name = 'John'");
// Returns: SELECT * FROM users WHERE name = $1

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
$protobuf = pg_query_scan('SELECT 1');
```

## Functions Reference

| Function | Description | Returns |
|----------|-------------|---------|
| `pg_query_parse(string $sql)` | Parse SQL to JSON AST | `string` (JSON) |
| `pg_query_fingerprint(string $sql)` | Generate query fingerprint | `string\|false` |
| `pg_query_normalize(string $sql)` | Normalize query with placeholders | `string\|false` |
| `pg_query_parse_plpgsql(string $sql)` | Parse PL/pgSQL function | `string` (JSON) |
| `pg_query_split(string $sql)` | Split multiple statements | `array<string>` |
| `pg_query_scan(string $sql)` | Scan SQL into tokens | `string` (protobuf) |

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

The extension is built on top of [libpg_query](https://github.com/pganalyze/libpg_query), which extracts PostgreSQL's query parser into a standalone library. This means you get the exact same SQL parsing behavior as PostgreSQL itself.

Key implementation details:
- **Static linking**: libpg_query.a is statically linked into the extension
- **Build dependency**: Requires `protobuf-c` library for compilation (libpg_query uses protobuf internally)
- **Auto-download**: The build system automatically downloads the correct libpg_query version

## See Also

- [pg-query library](/documentation/components/libs/pg-query.md) - Higher-level PHP wrapper with strongly-typed AST nodes
- [libpg_query](https://github.com/pganalyze/libpg_query) - The underlying C library
- [Nix Development Environment](/documentation/contributing/nix.md) - Using nix-shell for development
