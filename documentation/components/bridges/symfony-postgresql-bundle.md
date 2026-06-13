---
package: flow-php/symfony-postgresql-bundle
---

# Symfony PostgreSQL Bundle

Symfony bundle integrating Flow PHP's PostgreSQL library with Symfony applications, providing database management,
schema migrations, catalog-driven schema diffing, and optional telemetry for query tracing and metrics.

[PACKAGE_NAV]

[TOC]

## Installation

For detailed installation instructions, see
the [installation page](/documentation/installation/packages/symfony-postgresql-bundle.md).

## Overview

This bundle is built on top of [flow-php/postgresql](/documentation/components/libs/postgresql.md)
and [flow-php/pg-query-ext](/documentation/components/extensions/pg-query-ext.md) — see those pages for the underlying
client, query builders, catalog, and migration engine.

For telemetry support (tracing, metrics, query logging per connection),
the [Symfony Telemetry Bundle](/documentation/components/bridges/symfony-telemetry-bundle.md) is required and must
expose a configured `Telemetry` service referenced via `telemetry.service_id`.

This bundle integrates Flow PHP's PostgreSQL library with Symfony applications. It provides:

- **Multiple database connections** - Configure and manage several PostgreSQL connections independently
- **Database management commands** - Create, drop databases and execute SQL from the console
- **Migration framework** - Generate, execute, and track schema migrations
- **Schema diffing** - Automatically generate migrations by comparing the database to catalog definitions
- **Rollback support** - Generate reversible migrations with automatic rollback files
- **Telemetry integration** - Distributed tracing, query logging, and metrics per connection
- **Catalog provider system** - Define target schemas via PHP attributes, service references, or inline YAML

## Configuration Reference

### Connections

At least one connection is required. Each connection has a DSN and optional telemetry and migrations blocks.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'
```

### Connection Overrides

Each connection may override individual parts of the parsed DSN. Every key maps 1:1 onto an immutable
`ConnectionParameters` method in the `flow-php/postgresql` library; the bundle adds no logic of its own.
Overrides are applied at service-factory time, so values may be `%env(...)%` placeholders — they are
resolved at runtime, not when the configuration is parsed.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

      dbname: null            # Replaces the database name parsed from the DSN
      host: null              # Replaces the host parsed from the DSN
      port: null              # Replaces the port parsed from the DSN
      user: null              # Replaces the user parsed from the DSN
      password: null          # Replaces the password parsed from the DSN
      dbname_suffix: ''       # Appends the given suffix to the configured database name
```

| Key             | Type   | Default | Effect                                                    |
|-----------------|--------|---------|-----------------------------------------------------------|
| `dbname`        | string | `null`  | Replaces the database name parsed from the DSN.           |
| `host`          | string | `null`  | Replaces the host parsed from the DSN.                    |
| `port`          | int    | `null`  | Replaces the port parsed from the DSN.                    |
| `user`          | string | `null`  | Replaces the user parsed from the DSN.                    |
| `password`      | string | `null`  | Replaces the password parsed from the DSN.                |
| `dbname_suffix` | string | `''`    | Appends the given suffix to the configured database name. |

`dbname`/`host`/`port`/`user`/`password` are applied first, then `dbname_suffix` **last** — so
`dbname: 'foo'` + `dbname_suffix: '_test'` yields `foo_test`. A connection with only `dsn` behaves
exactly as before.

#### `dbname_suffix` for parallel tests

```yaml
# config/packages/flow_postgresql.yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_ANALYTICAL_URL)%'

when@test:
  flow_postgresql:
    connections:
      default:
        dbname_suffix: '_test%env(default::TEST_TOKEN)%'
```

### Telemetry

Enable telemetry per connection to get distributed tracing, query logging, and metrics.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

      telemetry:
        service_id: "flow.telemetry"         # Required: Telemetry service ID
        clock_service_id: null               # Optional: PSR-20 clock service (defaults to SystemClock)
        trace_queries: true                  # Record query execution in traces
        trace_transactions: true             # Record transaction boundaries
        collect_metrics: true                # Collect query metrics (duration, row count)
        log_queries: false                   # Log all queries
        max_query_length: 1000               # Truncate queries longer than this (chars)
        include_parameters: false            # Include query parameters in traces
        max_parameters: 10                   # Max number of parameters to include
        max_parameter_length: 100            # Max length of each parameter value (chars)
```

### Web Profiler

Adds a **Flow PostgreSQL** panel to the Symfony Web Profiler listing the SQL executed during the
current request — like the Doctrine bundle's Queries panel, but for the Flow PostgreSQL client and
independent of telemetry. Each connection's client is decorated with a recording wrapper that
captures statement, bound parameters, duration, returned/affected rows, the calling code location,
and failures.

```yaml
flow_postgresql:
  profiler:
    # enabled: null (default) auto-enables iff WebProfilerBundle is registered; true forces it on
    # (throws if WebProfilerBundle is absent); false forces it off entirely.
    enabled: ~
    include_parameters: true # show bound query parameters in the panel
```

Recording is dev-only and adds nothing in production: when the profiler is disabled — or
WebProfilerBundle is absent in `enabled: ~` mode — no connection is decorated. A single connection
can opt out while the profiler is on:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'
      profiler: false   # do not record this connection's queries (default: true)
```

### Migrations

Migrations are configured at the top level, not per connection. Use `--connection` to target a specific connection
when running migration commands.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  migrations:
    enabled: true
    directory: "%kernel.project_dir%/migrations"  # Where migration files are stored
    namespace: "App\\Migrations"                   # PHP namespace for generated migrations
    table_name: "flow_migrations"                  # Database table tracking executed migrations
    table_schema: "public"                         # Schema for the migrations table
    migration_file_name: "migration.php"           # Name of the migration file in each version directory
    rollback_file_name: "rollback.php"             # Name of the rollback file in each version directory
    all_or_nothing: false                          # Wrap all migrations in a single transaction
    generate_rollback: true                        # Generate rollback files automatically
    drop_if_exists: false                          # Emit IF EXISTS on diff-generated DROP statements
```

#### `drop_if_exists`

Set `drop_if_exists: true` to render every diff-generated `DROP` with `IF EXISTS` — useful for convergence migrations
that run against both fresh and legacy databases. Default `false`, so a missing object fails loudly (drift detection).
Override for a single run with the [`--drop-if-exists`](#generating-migrations-from-schema-diff) flag.

#### Accessing the service container in a migration

When migrations are enabled, the bundle injects the application's service container into the migration context under
`FlowPostgreSqlBundle::SERVICE_CONTAINER`. This gives migrations access to resolved parameters — including values
that Symfony resolves at container build time, such as decrypted secrets and `%env(...)%` processors — and to any
service, matching the behaviour of Doctrine's container-aware migrations.

Read it through `MigrationContext::attribute()`, which throws when the attribute is absent (use `hasAttribute()` to
check first):

```php
<?php

declare(strict_types=1);

use Flow\Bridge\Symfony\PostgreSqlBundle\FlowPostgreSqlBundle;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;
use Symfony\Component\DependencyInjection\ContainerInterface;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        /** @var ContainerInterface $container */
        $container = $context->attribute(FlowPostgreSqlBundle::SERVICE_CONTAINER);

        $dsn = $container->getParameter('app.analytics_database_url_readonly');

        // ... use the resolved value, e.g. to provision a role ...
    }

    public function transactional(): bool
    {
        return true;
    }
};
```

#### Passing custom attributes via configuration

Injecting the whole container forces any service a migration needs to be public. To avoid that, declare extra
attributes under `migrations.context` — they are merged into the migration context next to the container. Each value
can be a literal, an `@service_id` reference (use `@@` to keep a literal leading `@`), a `%parameter%` placeholder, or
a `%env(VAR)%` expression. Referenced services may be private.

```yaml
flow_postgresql:
  migrations:
    enabled: true
    context:
      report_generator: '@app.report_generator'        # service (may be private)
      readonly_url: '%env(DATABASE_READONLY_URL)%'      # resolved env
      batch_size: 500                                   # literal
```

Each key is read in a migration via `MigrationContext::attribute()`:

```php
<?php

declare(strict_types=1);

use App\Migration\ReportGenerator;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\MigrationContext;

return new class implements Migration {
    public function migrate(MigrationContext $context): void
    {
        /** @var ReportGenerator $reports */
        $reports = $context->attribute('report_generator');

        // ... use the injected service / value ...
    }

    public function transactional(): bool
    {
        return true;
    }
};
```

The `service_container` key is always present and cannot be overridden by a `context` entry.

### Catalog Providers

Catalog providers define the target database schema. When you run `flow:migrations:diff`, the bundle compares
the current database state to the catalog and generates migration SQL to reconcile the differences.

There are three ways to define catalog providers.

#### Attribute-Based Discovery

Annotate a class implementing `CatalogProvider` with `#[AsCatalogProvider]` for automatic discovery:

```php
<?php

namespace App\Database;

use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\PostgreSql\Catalog\Catalog;
use Flow\PostgreSql\Catalog\CatalogProvider;

#[AsCatalogProvider]
final class UsersCatalogProvider implements CatalogProvider
{
    public function get(): Catalog
    {
        return Catalog::create(
            // Define your tables, columns, indexes...
        );
    }
}
```

#### Service Reference

Reference an existing service by its ID:

```yaml
flow_postgresql:
  catalog_providers:
    - catalog_provider_id: "app.my_catalog_provider"
```

#### Inline YAML

Define the catalog directly in configuration:

```yaml
flow_postgresql:
  catalog_providers:
    - catalog:
        schemas:
          - name: "public"
            tables:
              - name: "users"
                columns:
                  - name: "id"
                    type: { name: "int4", schema: "pg_catalog" }
                    nullable: false
                  - name: "email"
                    type: { name: "varchar", schema: "pg_catalog" }
                    nullable: false
                  - name: "created_at"
                    type: { name: "timestamptz", schema: "pg_catalog" }
                    nullable: false
```

Multiple catalog providers can be combined. They are merged via `ChainCatalogProvider` into a single catalog.

### Exclusions

By default `flow:migrations:diff` compares **every** object found in the database against the catalog, so any
object that is not described by a catalog provider is reported as a difference to drop. That is a problem when some
objects are created outside of migrations — for example tables generated dynamically from user-uploaded content, or
a whole schema owned by another system. List those objects under `migrations.exclude` and they are skipped while the
diff is generated (the migrations tracking table is always excluded automatically).

```yaml
flow_postgresql:
  migrations:
    enabled: true
    exclude:
      - { schema: tenant_data }                  # exclude an entire schema and everything in it
      - { table: legacy_audit }                  # exclude a table by exact name
      - { starts_with: user_upload_ }            # exclude objects whose name starts with a prefix
      - { ends_with: _tmp, type: view }          # ...ending with a suffix, narrowed to views
      - { pattern: '/^cache_\d+$/', type: sequence } # ...matching a PCRE pattern, narrowed to sequences
      - { exact: scratch, for_schema: staging }  # exact name, narrowed to the "staging" schema
      - { policy_id: app.my_exclusion_policy }   # delegate to a custom ExclusionPolicy service
```

Each entry defines **exactly one** matcher:

| Key           | Excludes                                                            |
|---------------|--------------------------------------------------------------------|
| `schema`      | The named schema and every object inside it (tables, views, sequences, functions, …) |
| `table`       | A table by exact name (shorthand for `exact` narrowed to tables)   |
| `exact`       | Any object whose name matches exactly                              |
| `starts_with` | Any object whose name starts with the given prefix                 |
| `ends_with`   | Any object whose name ends with the given suffix                   |
| `pattern`     | Any object whose name matches the given PCRE pattern (with delimiters) |
| `policy_id`   | Delegates to a service implementing `Flow\PostgreSql\Schema\Exclusion\ExclusionPolicy` |

The `exact`, `starts_with`, `ends_with`, `pattern` and `policy_id` matchers can be narrowed with optional scopes:

- `type` — limit to a single object type: `table`, `view`, `materialized_view`, `sequence`, `function`,
  `procedure`, `domain` or `extension`. When omitted, the matcher applies to every type.
- `for_schema` — limit to a single schema. When omitted, the matcher applies to every schema.

Tables and whole schemas are filtered before they are introspected, so excluding dynamically created tables also
avoids the cost of reading their columns, indexes and constraints on every diff.

A custom `policy_id` service implements the same interface and receives every candidate object:

```php
<?php

namespace App\Database;

use Flow\PostgreSql\Schema\Exclusion\ExclusionPolicy;
use Flow\PostgreSql\Schema\Exclusion\SchemaObject;

final class MyExclusionPolicy implements ExclusionPolicy
{
    public function exclude(SchemaObject $object): bool
    {
        // $object->type, $object->schema and $object->name are available
        return str_contains($object->name ?? '', '__generated__');
    }
}
```

## Console Commands

### Database Commands

These commands are always available, regardless of migration configuration.

| Command                | Description                                       |
|------------------------|---------------------------------------------------|
| `flow:database:create` | Create the configured database                    |
| `flow:database:drop`   | Drop the configured database (requires `--force`) |
| `flow:sql:run`         | Execute SQL directly on the database              |

All commands accept `--connection` (`-c`) to target a specific connection.

### Migration Commands

These commands are available when `migrations.enabled: true` for at least one connection.

| Command                      | Description                                                   |
|------------------------------|---------------------------------------------------------------|
| `flow:migrations:diff`       | Generate a migration by comparing the database to the catalog |
| `flow:migrations:generate`   | Generate a blank migration                                    |
| `flow:migrations:migrate`    | Execute pending migrations                                    |
| `flow:migrations:execute`    | Execute a single migration version                            |
| `flow:migrations:status`     | View migration status                                         |
| `flow:migrations:current`    | Output the current migration version                          |
| `flow:migrations:latest`     | Output the latest available migrations                        |
| `flow:migrations:list`       | List all available migrations                                 |
| `flow:migrations:up-to-date` | Check if all migrations have been executed                    |

**Common options:**

| Option                | Description                                           |
|-----------------------|-------------------------------------------------------|
| `--connection` (`-c`) | Target a specific connection                          |
| `--dry-run`           | Preview changes without applying (migrate, execute)   |
| `--all-or-nothing`    | Wrap all migrations in a single transaction (migrate) |
| `--up` / `--down`     | Migration direction (execute)                         |
| `--allow-empty-diff`  | Don't fail when no changes detected (diff)            |
| `--from-empty-schema` | Generate as if the database were empty (diff)         |
| `--drop-if-exists`    | Emit `IF EXISTS` on generated DROP statements (diff)  |

## Migration Workflow

### Directory Structure

Migrations are organized as directories, one per version:

```
migrations/
  20260401120000_create_users/
    migration.php
    rollback.php
  20260402150000_add_orders/
    migration.php
    rollback.php
```

Each directory name follows the pattern `{version}` or `{version}_{name}`, where version is a numeric timestamp
(e.g. `20260401120000`) and name is an optional description.

### Generating Migrations from Schema Diff

Compare the current database to your catalog providers and generate migration SQL automatically:

```bash
php bin/console flow:migrations:diff
```

This creates a new versioned directory with `migration.php` containing the SQL to bring the database in sync
with the catalog, and `rollback.php` with the reverse operations (if `generate_rollback` is enabled).

Use `--from-empty-schema` to generate a migration as if the database were empty (useful for initial setup).

Use `--drop-if-exists` to emit `IF EXISTS` on every generated `DROP` for this run (a per-run override of the
[`drop_if_exists`](#drop_if_exists) config key):

```bash
php bin/console flow:migrations:diff --drop-if-exists
```

### Generating Blank Migrations

Create an empty migration for manual SQL (data migrations, custom operations):

```bash
php bin/console flow:migrations:generate
```

### Executing Migrations

Run all pending migrations:

```bash
php bin/console flow:migrations:migrate
```

Preview without applying:

```bash
php bin/console flow:migrations:migrate --dry-run
```

Execute a single version:

```bash
php bin/console flow:migrations:execute 20260401120000 --up
```

### Checking Status

```bash
php bin/console flow:migrations:status
php bin/console flow:migrations:up-to-date
```

## Multi-Connection Support

The bundle supports multiple independent PostgreSQL connections:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

    reporting:
      dsn: '%env(REPORTING_DATABASE_URL)%'

    analytics:
      dsn: '%env(ANALYTICS_DATABASE_URL)%'

  migrations:
    enabled: true
    directory: "%kernel.project_dir%/migrations"
```

Each connection gets its own set of services (`flow.postgresql.{name}.*`). The first connection is automatically
aliased to the base interfaces (`Client`, `ConnectionParameters`, `Migrator`, etc.), allowing direct type-hint
injection without specifying a connection name.

When migrations are enabled, migration services are registered for every connection. The same migration
directory and configuration is shared across all connections.

Target a specific connection with any command:

```bash
php bin/console flow:migrations:migrate --connection=reporting
```

## Symfony Messenger Integration

The bundle integrates
with [flow-php/symfony-postgresql-messenger-bridge](/documentation/components/bridges/symfony-postgresql-messenger-bridge.md)
to provide a Symfony Messenger transport backed by Flow's native PostgreSQL client — no Doctrine DBAL required.

### Setup

1. Install the messenger bridge:

```bash
composer require flow-php/symfony-postgresql-messenger-bridge:~--FLOW_PHP_VERSION--
```

2. Enable messenger:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  messenger:
    enabled: true
    table_name: messenger_messages  # default
    schema: public                  # default
```

3. Configure the Symfony Messenger transport:

```yaml
# config/packages/messenger.yaml
framework:
  messenger:
    transports:
      async:
        dsn: 'flow-pgsql://default'
    routing:
      App\Message\MyMessage: async
```

4. Generate and run the migration to create the messenger table:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

The `MessengerCatalogProvider` is registered automatically when `messenger.enabled: true`, so the `messenger_messages`
table appears in schema diffs alongside your other catalog-managed tables.

### Configuration Options

| Option              | Default              | Location                                   | Description                                            |
|---------------------|----------------------|--------------------------------------------|--------------------------------------------------------|
| `table_name`        | `messenger_messages` | `flow_postgresql.messenger`                | Table name in the database                             |
| `schema`            | `public`             | `flow_postgresql.messenger`                | Schema owning the table                                |
| `queue_name`        | `default`            | `framework.messenger.transports.*.options` | Queue name for message routing                         |
| `redeliver_timeout` | `3600`               | `framework.messenger.transports.*.options` | Seconds before unacknowledged messages are redelivered |

For full documentation, see
the [Symfony PostgreSQL Messenger Bridge](/documentation/components/bridges/symfony-postgresql-messenger-bridge.md).

## Symfony Cache Integration

The bundle integrates
with [flow-php/symfony-postgresql-cache-bridge](/documentation/components/bridges/symfony-postgresql-cache-bridge.md) to
provide PSR-6 / Symfony Cache pools backed by Flow's native PostgreSQL client — no Doctrine DBAL required. The adapter
implements `PruneableInterface`, so `cache:pool:prune` works out of the box.

Each pool is wired with the named connection's `ConnectionParameters` and opens its **own** `pg_connect`, isolated from
the `Client` services used by application code. This isolation is what keeps `$cache->save(...)` from accidentally
participating in (and being rolled back by) a transaction held by the caller.

### Setup

1. Install the cache bridge:

```bash
composer require flow-php/symfony-postgresql-cache-bridge:~--FLOW_PHP_VERSION--
```

2. Define one or more pools under `flow_postgresql.cache.pools`:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  cache:
    pools:
      app:
        connection: default            # optional; defaults to the first connection
        table_name: cache_app          # default: cache_items
        default_lifetime: 3600         # default: 0 (no expiry)

      sessions:
        table_name: cache_sessions
        namespace: 'sess.'
        default_lifetime: 86400
```

Each pool registers two services:

- `flow.postgresql.cache.pool.<name>` — the cache adapter (public).
- `flow.postgresql.cache.pool.<name>.catalog_provider` — tagged `flow.postgresql.catalog_provider`, so the table is
  included in `flow:migrations:diff`.

3. Wire the pools into Symfony's cache framework via `cache.adapter.psr6`:

```yaml
# config/packages/framework.yaml
framework:
  cache:
    pools:
      cache.app:
        adapter: cache.adapter.psr6
        provider: flow.postgresql.cache.pool.app

      cache.sessions:
        adapter: cache.adapter.psr6
        provider: flow.postgresql.cache.pool.sessions
```

The `cache.adapter.psr6` wrapper is required because Symfony's `CachePoolPass` overwrites the first constructor argument
of any service used directly as `adapter:`, which conflicts with this bridge's strict `Client` typing on argument 0.

4. Generate and run the migration to create the cache tables:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

### Configuration Options (per pool)

| Option                                              | Default                                                 | Description                                          |
|-----------------------------------------------------|---------------------------------------------------------|------------------------------------------------------|
| `connection`                                        | first connection                                        | `flow_postgresql.connections` key the pool uses      |
| `table_name`                                        | `cache_items`                                           | Table storing pool entries                           |
| `schema`                                            | `public`                                                | Schema owning the table                              |
| `id_col` / `data_col` / `lifetime_col` / `time_col` | `item_id` / `item_data` / `item_lifetime` / `item_time` | Column overrides                                     |
| `namespace`                                         | `''`                                                    | Cache pool namespace; chars in `[-+.A-Za-z0-9]` only |
| `default_lifetime`                                  | `0`                                                     | Default TTL in seconds; `0` means no expiry          |
| `marshaller_service_id`                             | `null`                                                  | Service ID of a custom `MarshallerInterface`         |
| `share_connection`                                  | `false`                                                 | Reuse the named connection's `Client` instead of opening a dedicated `pg_connect`. See "Sharing the Connection" below |

### Sharing the Connection

By default each pool opens its own `pg_connect` derived from the named connection's parameters. That isolation is what keeps `$cache->save(...)` from being rolled back if the caller's transaction fails.

Set `share_connection: true` to reuse the existing `flow.postgresql.<connection>.client` service instead. Trade-offs:

- **Pro:** one fewer `pg_connect` per worker per pool. Useful when connection budget is tight.
- **Con:** cache writes participate in whatever transaction the calling code happens to be inside. A rollback on the caller's transaction also rolls back the cache write.

Use `share_connection: true` only when you have an explicit reason to share state (e.g. transactional outbox, cache writes that should logically belong to the caller's unit of work).

### Pruning

Schedule the standard Symfony command on a cron to remove expired rows:

```bash
php bin/console cache:pool:prune
```

Without pruning, expired rows accumulate in the table. They are filtered out on read but are not deleted until either
the same key is fetched again or `cache:pool:prune` runs.

For full documentation, see
the [Symfony PostgreSQL Cache Bridge](/documentation/components/bridges/symfony-postgresql-cache-bridge.md).

## Symfony Session Integration

The bundle integrates
with [flow-php/symfony-postgresql-session-bridge](/documentation/components/bridges/symfony-postgresql-session-bridge.md)
to provide a Symfony session handler backed by Flow's native PostgreSQL client — no PDO or Doctrine DBAL required. The
table layout is byte-compatible with `PdoSessionHandler`, so existing session rows can be reused as-is.

The handler is wired with the named connection's `ConnectionParameters` and opens its **own** `pg_connect`, isolated
from the `Client` services used by application code. This is what makes `LOCK_TRANSACTIONAL` safe by default — the
session's transaction lives on a dedicated connection and never wraps unrelated request work.

### Setup

1. Install the session bridge:

```bash
composer require flow-php/symfony-postgresql-session-bridge:~--FLOW_PHP_VERSION--
```

2. Enable the session handler:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  session:
    enabled: true
    connection: default            # optional; defaults to the first connection
    table_name: sessions           # default
    schema: public                 # default
    lock_mode: transactional       # one of: none | advisory | transactional
    ttl: 86400                     # optional; falls back to session.gc_maxlifetime
```

Enabling the section registers three services:

- `flow.postgresql.session.handler` — the `FlowPostgreSqlSessionHandler` (public, also aliased to
  `\SessionHandlerInterface`).
- `flow.postgresql.session.catalog_provider` — tagged `flow.postgresql.catalog_provider`, so the `sessions` table is
  included in `flow:migrations:diff`.
- `flow.postgresql.session.purge_command` — see [Purging Sessions](#purging-sessions).

3. Wire the handler into Symfony's session framework:

```yaml
# config/packages/framework.yaml
framework:
  session:
    handler_id: flow.postgresql.session.handler
    cookie_secure: auto
    cookie_samesite: lax
```

4. Generate and run the migration to create the sessions table:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

### Configuration Options

| Option         | Default                       | Description                                                                          |
|----------------|-------------------------------|--------------------------------------------------------------------------------------|
| `enabled`      | `false`                       | Master switch for the session integration                                            |
| `connection`   | first connection              | `flow_postgresql.connections` key the handler uses                                   |
| `table_name`   | `sessions`                    | Table storing sessions                                                               |
| `schema`       | `public`                      | Schema owning the table                                                              |
| `id_col`       | `sess_id`                     | Column override                                                                      |
| `data_col`     | `sess_data`                   | Column override                                                                      |
| `lifetime_col` | `sess_lifetime`               | Column override                                                                      |
| `time_col`     | `sess_time`                   | Column override                                                                      |
| `lock_mode`    | `transactional`               | One of `none`, `advisory`, `transactional`. See bridge docs for the trade-offs       |
| `ttl`          | `null`                        | Session lifetime in seconds; `null` falls back to ini `session.gc_maxlifetime`       |
| `share_connection` | `false`                   | Reuse the named connection's `Client` instead of opening a dedicated `pg_connect`. See "Sharing the Connection" below |

### Sharing the Connection

By default the handler opens its own `pg_connect` derived from the named connection's parameters. That isolation is what keeps the session handler's transactions / locks from leaking into the connection used by application code.

Set `share_connection: true` to reuse the existing `flow.postgresql.<connection>.client` service instead. Use only when you have an explicit reason — tight connection budget or a deliberate need to keep session state on the same connection as application code.

### Purging Sessions

PHP's normal probabilistic GC (`session.gc_probability`) already triggers expired-row cleanup at request close, so most
applications don't need extra wiring. For deterministic cleanup on a cron, the bundle exposes:

```bash
# Delete only sessions whose sess_lifetime is in the past (default behavior)
php bin/console flow:postgresql:session:purge

# Or explicitly:
php bin/console flow:postgresql:session:purge --expired

# Wipe every session (logs everyone out — destructive)
php bin/console flow:postgresql:session:purge --all
```

`--expired` and `--all` are mutually exclusive; passing both returns a non-zero exit code.

For full documentation, see
the [Symfony PostgreSQL Session Bridge](/documentation/components/bridges/symfony-postgresql-session-bridge.md).

## Test Transaction Rollback

The bundle integrates
with [flow-php/phpunit-postgresql-bridge](/documentation/components/bridges/phpunit-postgresql-bridge.md) to
automatically wrap each PHPUnit test in a database transaction and roll it back after the test finishes — keeping your
test database clean without manual teardown.

### Setup

1. Install the PHPUnit bridge:

```bash
composer require --dev flow-php/phpunit-postgresql-bridge:~--FLOW_PHP_VERSION--
```

2. Register the PHPUnit extension in `phpunit.xml.dist`:

```xml

<extensions>
    <bootstrap class="Flow\Bridge\PHPUnit\PostgreSQL\PostgreSQLExtension"/>
</extensions>
```

3. Enable transaction rollback per connection in a test-environment config:

```yaml
# config/packages/test/flow_postgresql.yaml
flow_postgresql:
  connections:
    default:
      test_transaction_rollback: true
    readonly:
      test_transaction_rollback: false  # default, no wrapping
```

When `test_transaction_rollback` is `true`, the bundle replaces `PgSqlClient::connect` with `StaticClient::connect` for
that connection. This ensures the exact same `Client` instance is reused across kernel reboots within the same test, so
the transaction started by the PHPUnit extension covers all database operations performed by your services.

### How It Works

1. PHPUnit starts → extension enables `StaticClient` caching
2. Before each test → extension rolls back the previous transaction and begins a new one
3. Symfony kernel boots → bundle creates the client via `StaticClient::connect()` → returns the cached instance with an
   active transaction
4. Test runs → all queries go through the same cached client, inside the transaction
5. Next test starts → extension rolls back all changes from the previous test

PostgreSQL supports transactional DDL, so even `CREATE TABLE` and `ALTER TABLE` statements are rolled back.

### Skipping Rollback

Use the `#[SkipTransactionRollback]` attribute to opt out for specific tests, classes, or abstract parent classes. See
the [PHPUnit PostgreSQL Bridge documentation](/documentation/components/bridges/phpunit-postgresql-bridge.md) for
details.

## Complete Example

```yaml
# config/packages/flow_postgresql.yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

      telemetry:
        service_id: "flow.telemetry"
        trace_queries: true
        trace_transactions: true
        collect_metrics: true
        log_queries: false
        max_query_length: 1000

  messenger:
    enabled: true

  cache:
    pools:
      app:
        table_name: cache_app
        default_lifetime: 3600

  migrations:
    enabled: true
    directory: "%kernel.project_dir%/migrations"
    namespace: "App\\Migrations"
    table_name: "flow_migrations"
    table_schema: "public"
    all_or_nothing: false
    generate_rollback: true

  catalog_providers:
    - catalog:
        schemas:
          - name: "public"
            tables:
              - name: "users"
                columns:
                  - name: "id"
                    type: { name: "int4", schema: "pg_catalog" }
                    nullable: false
                  - name: "email"
                    type: { name: "varchar", schema: "pg_catalog" }
                    nullable: false
                  - name: "created_at"
                    type: { name: "timestamptz", schema: "pg_catalog" }
                    nullable: false
```
