# ETL Adapter: PostgreSQL

- [⬅️️ Back](/documentation/introduction.md)
- [📦Packagist](https://packagist.org/packages/flow-php/etl-adapter-postgresql)
- [🐙GitHub](https://github.com/flow-php/etl-adapter-postgresql)
- [📚API Reference](/documentation/api/adapter/postgresql)
- [📁Files](/documentation/api/adapter/postgresql/indices/files.html)

[TOC]

Flow PHP's Adapter PostgreSQL is designed to seamlessly integrate PostgreSQL within your ETL (Extract, Transform, Load) workflows. This adapter is built on top of the [PostgreSQL library](/documentation/components/libs/postgresql/client-connection.md), providing efficient data extraction capabilities with built-in pagination support. By harnessing the Adapter PostgreSQL library, developers can tap into robust features for precise database interaction, simplifying complex data transformations and enhancing data processing efficiency.

## Installation

```
composer require flow-php/etl-adapter-postgresql:~--FLOW_PHP_VERSION--
```

## Requirements

- PHP 8.3+
- ext-pgsql
- ext-pg_query (optional, for query builder support)

## Description

This adapter provides two extraction strategies optimized for different use cases:

- **LIMIT/OFFSET Pagination**: Simple pagination suitable for smaller datasets
- **Keyset (Cursor) Pagination**: Efficient pagination for large datasets with consistent performance

Both extractors support:
- Raw SQL strings or Query Builder objects
- Configurable page sizes
- Maximum row limits
- Custom schema definitions

## Extractor - LIMIT/OFFSET Pagination

The `from_pgsql_limit_offset` extractor uses traditional LIMIT/OFFSET pagination. This is simple to use but may have performance degradation on very large datasets with high offsets.

### Basic Usage

```php
use function Flow\ETL\Adapter\PostgreSql\from_pgsql_limit_offset;
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$client = pgsql_client(pgsql_connection_dsn('pgsql://user:pass@localhost:5432/database'));

data_frame()
    ->read(from_pgsql_limit_offset(
        $client,
        "SELECT id, name, email FROM users ORDER BY id",
        pageSize: 1000
    ))
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
        pageSize: 500
    ))
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
        pageSize: 1000,
        maximum: 10000  // Only extract first 10,000 rows
    ))
    ->write(to_output())
    ->run();
```

## Extractor - Keyset (Cursor) Pagination

The `from_pgsql_key_set` extractor uses keyset pagination (also known as cursor-based pagination). This provides consistent performance regardless of how deep you paginate, making it ideal for large datasets.

### Basic Usage

```php
use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set, pgsql_pagination_key_asc, pgsql_pagination_key_set};

data_frame()
    ->read(from_pgsql_key_set(
        $client,
        "SELECT id, name, email FROM users",
        pgsql_pagination_key_set(pgsql_pagination_key_asc('id')),
        pageSize: 1000
    ))
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
        pageSize: 500
    ))
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
        pageSize: 1000
    ))
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
        pageSize: 500
    ))
    ->write(to_output())
    ->run();
```

## DSL Functions Reference

### Extractor Functions

| Function | Description |
|----------|-------------|
| `from_pgsql_limit_offset($client, $query, $pageSize, $maximum)` | Extract using LIMIT/OFFSET pagination |
| `from_pgsql_key_set($client, $query, $keySet, $pageSize, $maximum)` | Extract using keyset pagination |

### Key Functions

| Function | Description |
|----------|-------------|
| `pgsql_pagination_key_asc($column)` | Create an ascending key for keyset pagination |
| `pgsql_pagination_key_desc($column)` | Create a descending key for keyset pagination |
| `pgsql_pagination_key_set(...$keys)` | Create a keyset from one or more keys |

## Choosing Between Extractors

### Use LIMIT/OFFSET when:
- Working with smaller datasets (< 100k rows)
- You need simple, straightforward pagination
- Random page access is required
- The offset values remain relatively small

### Use Keyset Pagination when:
- Working with large datasets (100k+ rows)
- Performance consistency is critical
- You're doing sequential/forward pagination
- Your table has suitable indexed columns for the keyset

## Performance Considerations

### LIMIT/OFFSET

- Simple to understand and implement
- Performance degrades as offset increases (PostgreSQL must skip all previous rows)
- Memory usage increases with larger offsets

### Keyset Pagination

- Consistent O(1) performance regardless of position
- Requires indexed columns in the keyset
- Cannot jump to arbitrary pages (sequential access only)
- Handles concurrent modifications more gracefully
