---
package: flow-php/symfony-postgresql-cache-bridge
---

# Symfony PostgreSQL Cache Bridge

A Symfony Cache adapter on the native PostgreSQL library of Flow PHP. It replaces `symfony/doctrine-dbal-adapter` and does not need Doctrine DBAL. Cache items are stored directly in PostgreSQL with the query builder and client of Flow.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-cache-bridge:~--FLOW_PHP_VERSION--
```

For the Symfony framework integration (pool registration from configuration, automatic migrations), use this bridge through the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md). This page covers the standalone usage.

## How It Works

`FlowPostgreSqlCacheAdapter` extends the `AbstractAdapter` of Symfony and implements `PruneableInterface`. Items are stored in one PostgreSQL table with `item_id`, `item_data` (`bytea`), `item_lifetime` (`int`, nullable), and `item_time` (`int`).

- A save is an `INSERT ... ON CONFLICT (item_id) DO UPDATE SET ...` inside a transaction. A nested transaction becomes a `SAVEPOINT` automatically.
- Reads project rows through `CASE WHEN item_lifetime IS NULL OR item_lifetime + item_time > now() THEN item_data ELSE NULL END`. Expired rows still come back from Postgres, but with `data = NULL`. The adapter treats them as misses and deletes them in the same request.
- `prune()` runs one bulk `DELETE` of all expired rows, optionally only in the pool namespace.
- By default, marshalling uses the `DefaultMarshaller` of Symfony. To override it, give a custom `MarshallerInterface` to the constructor.

## Connection Ownership

The adapter accepts `ConnectionParameters` or a `Client`:

- Give `ConnectionParameters` *(recommended default)*: the adapter opens (and owns) its own `pg_connect`. Cache writes are never part of the transactions on a `Client` that the application code also uses. A rollback of the caller therefore cannot silently undo cached values.
- Give a `Client`: the adapter uses the given connection. The caller is responsible for the lifetime and the transaction semantics. Use this only for a known reason, for example a tight connection budget or an explicit need to share state.

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

| Key               | Default         |
|-------------------|-----------------|
| `db_table`        | `cache_items`   |
| `db_schema`       | `public`        |
| `db_id_col`       | `item_id`       |
| `db_data_col`     | `item_data`     |
| `db_lifetime_col` | `item_lifetime` |
| `db_time_col`     | `item_time`     |

## Pruning

Postgres has no row-level TTL, so one of two paths removes expired rows:

- Lazy: a read of the same key deletes that one row.
- Explicit: `$cache->prune()` deletes all expired rows in bulk. In a Symfony app with the adapter, `bin/console cache:pool:prune` does the same.

Without periodic pruning, dead rows collect in the table, also though readers do not see them.

## Table Schema

`CacheCatalogProvider` produces this layout:

| Column          | Type                       | Description                            |
|-----------------|----------------------------|----------------------------------------|
| `item_id`       | `varchar(255) PRIMARY KEY` | Cache key                              |
| `item_data`     | `bytea NOT NULL`           | Marshalled payload                     |
| `item_lifetime` | `int` (nullable)           | TTL in seconds. `NULL` means no expiry |
| `item_time`     | `int NOT NULL`             | Unix timestamp of the write of the row |

A composite index on `(item_lifetime, item_time)` makes `prune()` scans faster.
