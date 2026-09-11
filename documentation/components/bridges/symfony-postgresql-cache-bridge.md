---
package: flow-php/symfony-postgresql-cache-bridge
---

# Symfony PostgreSQL Cache Bridge

A Symfony Cache adapter backed by Flow PHP's native PostgreSQL library. Replaces `symfony/doctrine-dbal-adapter` without requiring Doctrine DBAL - cache items are stored directly in PostgreSQL using Flow's query builder and client.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-cache-bridge:~--FLOW_PHP_VERSION--
```

For Symfony framework integration (config-driven pool registration, automatic migrations) use this bridge through the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md). The page below covers standalone usage.

## How It Works

`FlowPostgreSqlCacheAdapter` extends Symfony's `AbstractAdapter` and implements `PruneableInterface`. Items are stored in a single PostgreSQL table with `item_id`, `item_data` (`bytea`), `item_lifetime` (`int`, nullable), and `item_time` (`int`).

- **Saves** are issued as `INSERT ... ON CONFLICT (item_id) DO UPDATE SET ...` inside a transaction (which converts to `SAVEPOINT` automatically when nested).
- **Reads** project rows through `CASE WHEN item_lifetime IS NULL OR item_lifetime + item_time > now() THEN item_data ELSE NULL END`. Expired rows still come back from Postgres but with `data = NULL`; the adapter treats them as misses and deletes them in the same request.
- **`prune()`** runs a single bulk `DELETE` of every expired row, optionally restricted to the pool namespace.
- **Marshalling** uses Symfony's `DefaultMarshaller` by default; pass a custom `MarshallerInterface` to the constructor to override.

## Connection Ownership

The adapter accepts either `ConnectionParameters` or a `Client`:

- **Pass `ConnectionParameters`** *(recommended default)* - the adapter opens (and owns) its own `pg_connect`. Cache writes never participate in transactions running on a `Client` shared with application code, so a caller's rollback can't silently undo cached values.
- **Pass a `Client`** - the adapter reuses the supplied connection. Lifetime and transaction semantics are the caller's responsibility. Use this only when you know what you are doing (tight connection budget, explicit need to share state).

## Usage

```php
use Flow\Bridge\Symfony\PostgreSQLCache\{CacheCatalogProvider, FlowPostgreSqlCacheAdapter};
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$params = pgsql_connection_dsn(getenv('DATABASE_URL'));

// Create the table once (or manage it via your migration tool of choice).
// The setup client below is unrelated to the connection the adapter owns.
$setup = pgsql_client($params);
foreach ((new CacheCatalogProvider())->get()->get('public')->table('cache_items')->toSql() as $sql) {
    $setup->execute($sql);
}
$setup->close();

$cache = new FlowPostgreSqlCacheAdapter($params, namespace: 'app', defaultLifetime: 3600);

$item = $cache->getItem('greeting');

if (!$item->isHit()) {
    $item->set('hello world');
    $item->expiresAfter(600);
    $cache->save($item);
}

echo $cache->getItem('greeting')->get();
```

## Constructor

```php ignore
new FlowPostgreSqlCacheAdapter(
    ConnectionParameters|Client $connection,
    string $namespace = '',
    int $defaultLifetime = 0,
    array $options = [],
    ?MarshallerInterface $marshaller = null,
);
```

`$options` accepts the column / table overrides:

| Key | Default |
|-----|---------|
| `db_table` | `cache_items` |
| `db_schema` | `public` |
| `db_id_col` | `item_id` |
| `db_data_col` | `item_data` |
| `db_lifetime_col` | `item_lifetime` |
| `db_time_col` | `item_time` |

## Pruning

Postgres has no row-level TTL, so expired rows are removed by either of two paths:

- **Lazy** - a read of the same key triggers a delete of that one row.
- **Explicit** - calling `$cache->prune()` (or `bin/console cache:pool:prune` when the adapter is wired into a Symfony app) bulk-deletes everything past its expiry.

Without periodic pruning, dead rows accumulate in the table even though they are invisible to readers.

## Table Schema

`CacheCatalogProvider` produces this layout:

| Column | Type | Description |
|--------|------|-------------|
| `item_id` | `varchar(255) PRIMARY KEY` | Cache key |
| `item_data` | `bytea NOT NULL` | Marshalled payload |
| `item_lifetime` | `int` (nullable) | TTL in seconds; `NULL` means no expiry |
| `item_time` | `int NOT NULL` | Unix timestamp when the row was written |

A composite index on `(item_lifetime, item_time)` speeds up `prune()` scans.
