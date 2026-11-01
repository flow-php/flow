---
package: flow-php/symfony-postgresql-session-bridge
---

# Symfony PostgreSQL Session Bridge

A Symfony session handler on the native PostgreSQL library of Flow PHP. It replaces `PdoSessionHandler` and does not need PDO or Doctrine DBAL. Sessions are stored directly in PostgreSQL with the query builder and client of Flow.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-session-bridge:~--FLOW_PHP_VERSION--
```

For the Symfony framework integration (handler registration from configuration, automatic migrations, purge command), use this bridge through the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md). This page covers the standalone usage.

## How It Works

`FlowPostgreSqlSessionHandler` extends the `AbstractSessionHandler` of Symfony. Sessions are stored in one PostgreSQL table. Its schema is byte-compatible with the default layout of `PdoSessionHandler`: `sess_id`, `sess_data` (`bytea`), `sess_lifetime` (`int`, absolute Unix expiry), `sess_time` (`int`).

- Reads select `sess_data` and `sess_lifetime` for the requested session id. Rows with `sess_lifetime < now()` are misses.
- A write is an `INSERT ... ON CONFLICT (sess_id) DO UPDATE SET ...` with `sess_lifetime = now() + ttl`.
- `gc()` records the expiry cutoff of the request. It delays the real `DELETE FROM sessions WHERE sess_lifetime < now()` until `close()`. This is the same contract as in `PdoSessionHandler`. The delete therefore does not run while the session row is locked.
- `purgeExpired()` / `purgeAll()` are explicit maintenance methods outside the request lifecycle.

## Connection Ownership

The handler accepts `ConnectionParameters` or a `Client`:

- Give `ConnectionParameters` *(recommended default)*: the handler opens (and owns) its own `pg_connect`. The connection opens lazily, at the first use. `close()` releases it, together with the rest of the request state. Safe `LOCK_TRANSACTIONAL` semantics need this option.
- Give a `Client`: the handler uses the given connection and never closes it (the caller owns the lifetime). Use this only for a known reason, for example a tight connection budget or an explicit need to share state.

## Usage

```php
use Flow\Bridge\Symfony\PostgreSQLSession\{FlowPostgreSqlSessionHandler, SessionCatalogProvider};
use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$params = pgsql_connection_dsn(getenv('DATABASE_URL'));

// Create the table once (or manage it via your migration tool of choice).
// The setup client below is unrelated to the connection the handler owns.
$setup = pgsql_client($params);
foreach ((new SessionCatalogProvider())->get()->get('public')->table('sessions')->toSql() as $sql) {
    $setup->execute($sql);
}
$setup->close();

$handler = new FlowPostgreSqlSessionHandler($params, [
    'lock_mode' => FlowPostgreSqlSessionHandler::LOCK_TRANSACTIONAL,
    'ttl' => 86400,
]);

session_set_save_handler($handler, true);
```

## Constructor

```php ignore
new FlowPostgreSqlSessionHandler(
    ConnectionParameters|Client $connection,
    array $options = [],
);
```

`$options` accepts:

| Key               | Default                                                    |
|-------------------|------------------------------------------------------------|
| `db_table`        | `sessions`                                                 |
| `db_schema`       | `public`                                                   |
| `db_id_col`       | `sess_id`                                                  |
| `db_data_col`     | `sess_data`                                                |
| `db_lifetime_col` | `sess_lifetime`                                            |
| `db_time_col`     | `sess_time`                                                |
| `lock_mode`       | `LOCK_TRANSACTIONAL`                                       |
| `ttl`             | `null` (falls back to `ini_get('session.gc_maxlifetime')`) |

## Locking Modes

Three constants control how concurrent requests for the same session id are serialized. They are the same as in `PdoSessionHandler`:

- `LOCK_TRANSACTIONAL` *(default)*: `doRead` opens a transaction with `SELECT ... FOR UPDATE` on the row. `doWrite` / `doDestroy` commits it. This is the strongest guarantee, for the row.
- `LOCK_ADVISORY`: takes `pg_advisory_lock(<session_id_as_bigint>)` before the read, and releases it on write/destroy/close. The session id becomes a bigint with the same scheme as in `PdoSessionHandler`: the first 8 bytes as little-endian. Applications that use both handlers therefore lock the same key.
- `LOCK_NONE`: no locking. The last write wins. Use it only with proven serial access.

## Cleaning Up Expired Rows

PHP calls `SessionHandlerInterface::gc()` at random, as `session.gc_probability` sets. The handler delays the real delete to `close()`, so the normal GC cycle of PHP works without more wiring.

For deterministic cleanup, for example on a cron, call `purgeExpired()` directly. To delete everything, call `purgeAll()`. It runs `TRUNCATE` and returns `0`, because PostgreSQL does not report row counts for that operation.

## Table Schema

`SessionCatalogProvider` produces this layout:

| Column          | Type                       | Description                               |
|-----------------|----------------------------|-------------------------------------------|
| `sess_id`       | `varchar(128) PRIMARY KEY` | PHP session id                            |
| `sess_data`     | `bytea NOT NULL`           | Serialized session payload                |
| `sess_lifetime` | `int NOT NULL`             | Absolute Unix timestamp of the row expiry |
| `sess_time`     | `int NOT NULL`             | Unix timestamp of the last write          |

One index on `sess_lifetime` keeps `gc()` / `purgeExpired()` cheap.
