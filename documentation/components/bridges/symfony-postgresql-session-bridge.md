---
package: flow-php/symfony-postgresql-session-bridge
---

# Symfony PostgreSQL Session Bridge

A Symfony session handler backed by Flow PHP's native PostgreSQL library. Replaces `PdoSessionHandler` without requiring PDO or Doctrine DBAL - sessions are stored directly in PostgreSQL using Flow's query builder and client.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-session-bridge:~--FLOW_PHP_VERSION--
```

For Symfony framework integration (config-driven handler registration, automatic migrations, purge command) use this bridge through the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md). The page below covers standalone usage.

## How It Works

`FlowPostgreSqlSessionHandler` extends Symfony's `AbstractSessionHandler`. Sessions are stored in a single PostgreSQL table whose schema is byte-compatible with `PdoSessionHandler`'s default layout: `sess_id`, `sess_data` (`bytea`), `sess_lifetime` (`int`, absolute Unix expiry), `sess_time` (`int`).

- **Reads** select `sess_data` and `sess_lifetime` for the requested session id; rows whose `sess_lifetime < now()` are treated as misses.
- **Writes** are issued as `INSERT ... ON CONFLICT (sess_id) DO UPDATE SET ...` with `sess_lifetime = now() + ttl`.
- **`gc()`** records the request's expiry cutoff and defers the actual `DELETE FROM sessions WHERE sess_lifetime < now()` until `close()` - same contract as `PdoSessionHandler`, so the deletion does not run while the session row is locked.
- **`purgeExpired()` / `purgeAll()`** are explicit maintenance methods that bypass the request lifecycle.

## Connection Ownership

The handler accepts either `ConnectionParameters` or a `Client`:

- **Pass `ConnectionParameters`** *(recommended default)* - the handler opens (and owns) its own `pg_connect`. The connection is created lazily on first use; `close()` releases it along with the rest of the per-request state. Required for safe `LOCK_TRANSACTIONAL` semantics.
- **Pass a `Client`** - the handler reuses the supplied connection and never closes it (the caller owns the lifetime). Use this only when you know what you are doing (tight connection budget, explicit need to share state).

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

| Key | Default |
|-----|---------|
| `db_table` | `sessions` |
| `db_schema` | `public` |
| `db_id_col` | `sess_id` |
| `db_data_col` | `sess_data` |
| `db_lifetime_col` | `sess_lifetime` |
| `db_time_col` | `sess_time` |
| `lock_mode` | `LOCK_TRANSACTIONAL` |
| `ttl` | `null` (falls back to `ini_get('session.gc_maxlifetime')`) |

## Locking Modes

Three constants control how concurrent requests for the same session id are serialized. They mirror `PdoSessionHandler`:

- **`LOCK_TRANSACTIONAL`** *(default)* - `doRead` opens a transaction with `SELECT ... FOR UPDATE` on the row; `doWrite` / `doDestroy` commits it. Strongest guarantee, scoped to the row.
- **`LOCK_ADVISORY`** - acquires `pg_advisory_lock(<session_id_as_bigint>)` before reading, releases it on write/destroy/close. The session id is converted to a bigint using the same first-8-bytes-as-little-endian scheme as `PdoSessionHandler`, so applications mixing both handlers will collide on the same key.
- **`LOCK_NONE`** - no locking. Last write wins. Use only when you can prove serial access.

## Cleaning Up Expired Rows

PHP triggers `SessionHandlerInterface::gc()` probabilistically based on `session.gc_probability`. The handler defers the actual delete to `close()`, so PHP's normal GC cycle works without further wiring.

For deterministic cleanup (e.g. on a cron) call `purgeExpired()` directly. To wipe everything, call `purgeAll()` - it runs `TRUNCATE` and returns `0` because PostgreSQL does not report row counts for that operation.

## Table Schema

`SessionCatalogProvider` produces this layout:

| Column | Type | Description |
|--------|------|-------------|
| `sess_id` | `varchar(128) PRIMARY KEY` | PHP session id |
| `sess_data` | `bytea NOT NULL` | Serialized session payload |
| `sess_lifetime` | `int NOT NULL` | Absolute Unix timestamp at which the row expires |
| `sess_time` | `int NOT NULL` | Unix timestamp of the last write |

A single index on `sess_lifetime` keeps `gc()` / `purgeExpired()` cheap.
