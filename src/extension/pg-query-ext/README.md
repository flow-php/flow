# pg_query PHP Extension

A compiled PHP extension for PostgreSQL query parsing using [libpg_query](https://github.com/pganalyze/libpg_query).

## Features

- Parse PostgreSQL SQL queries into JSON AST
- Generate query fingerprints for query grouping
- Normalize SQL queries (replace literals with placeholders)
- Parse PL/pgSQL functions
- Split multiple SQL statements
- Scan SQL into tokens

## Installation

### Using PIE (Recommended)

[PIE](https://github.com/php/pie) is the modern PHP extension installer.

**Prerequisites:** Install protobuf-c library on your system:

```bash
# Ubuntu/Debian
sudo apt-get install libprotobuf-c-dev git make gcc

# macOS with Homebrew
brew install protobuf-c

# Fedora/RHEL
sudo dnf install protobuf-c-devel git make gcc
```

**Install the extension:**

```bash
# Simple installation (auto-downloads libpg_query)
pie install flow-php/pg-query-ext

# Or with a pre-installed libpg_query (must be version 17+)
pie install flow-php/pg-query-ext --with-pg-query=/usr/local
```

The extension will automatically download and build libpg_query 17-latest if not found on your system.

> **Note:** This extension uses PostgreSQL 17 grammar (libpg_query 17-latest). It requires `postgres_deparse.h` which is only available in libpg_query 17+.

### Requirements

- PHP 8.2+
- C compiler (gcc/clang)
- git (for auto-downloading libpg_query)
- make
- protobuf-c library

### Manual Build

```bash
cd src/extension/pg-query-ext

# This will download and build libpg_query, then build the extension
make build

# Run extension tests
make test

# Install to system PHP (optional)
make install
```

### Using Nix

From the Flow PHP monorepo root:

```bash
# Enter Nix shell with pg_query extension loaded and build tools available
nix-shell --arg with-pg-query-ext true

# Navigate to extension directory
cd src/extension/pg-query-ext

# Build and test
make test
```

## Usage

```php
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

// Scan SQL into tokens (returns protobuf data)
$protobuf = pg_query_scan('SELECT 1');
```

## Loading the Extension

### During Development

```bash
php -d extension=./ext/modules/pg_query.so your_script.php
```

### In php.ini

```ini
extension=pg_query
```

## Functions Reference

| Function                              | Description                       | Returns             |
|---------------------------------------|-----------------------------------|---------------------|
| `pg_query_parse(string $sql)`         | Parse SQL to JSON AST             | `string` (JSON)     |
| `pg_query_fingerprint(string $sql)`   | Generate query fingerprint        | `string\|false`     |
| `pg_query_normalize(string $sql)`     | Normalize query with placeholders | `string\|false`     |
| `pg_query_parse_plpgsql(string $sql)` | Parse PL/pgSQL function           | `string` (JSON)     |
| `pg_query_split(string $sql)`         | Split multiple statements         | `array<string>`     |
| `pg_query_scan(string $sql)`          | Scan SQL into tokens              | `string` (protobuf) |

## Development

```bash
# Build and run tests
make test

# Rebuild extension only (without rebuilding libpg_query)
make rebuild

# Clean build artifacts
make clean

# Remove everything including libpg_query
make distclean
```

## License

MIT
