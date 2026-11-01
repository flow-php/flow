---
package: flow-php/symfony-postgresql-bundle
---

# Symfony PostgreSQL Bundle

This Symfony bundle connects the Flow PHP PostgreSQL library to Symfony applications. It gives database management,
schema migrations, schema diffs against a catalog, and optional telemetry for query tracing and metrics.

[PACKAGE_NAV]

[TOC]

## Installation

The [installation page](/documentation/installation/packages/symfony-postgresql-bundle.md) has the installation
instructions.

## Overview

This bundle is built on top of [flow-php/postgresql](/documentation/components/libs/postgresql.md)
and [flow-php/pg-query-ext](/documentation/components/extensions/pg-query-ext.md). Those pages describe the
client, the query builders, the catalog, and the migration engine.

The [Symfony Telemetry Bundle](/documentation/components/bridges/symfony-telemetry-bundle.md) is required for
telemetry support (tracing, metrics, and query logging per connection). It must expose a configured `Telemetry`
service. The `telemetry.service_id` key references that service.

The bundle has these features:

- Multiple database connections: configure and manage several PostgreSQL connections independently.
- Database management commands: create and drop databases, and run SQL from the console.
- Migration framework: generate, execute, and track schema migrations.
- Schema diffs: generate migrations automatically from a comparison of the database with the catalog definitions.
- Rollback support: generate reversible migrations with automatic rollback files.
- Telemetry integration: distributed tracing, query logging, and metrics per connection.
- Catalog provider system: define target schemas with PHP attributes, service references, or inline YAML.

## Configuration Reference

### Connections

You must configure at least one connection. Each connection has a DSN and an optional telemetry block. `migrations` is a top-level key.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'
```

### Connection Overrides

Each connection can override individual parts of the parsed DSN. Each key maps 1:1 to a method of the immutable
`ConnectionParameters` class in the `flow-php/postgresql` library. The bundle adds no logic of its own.
The service factory applies the overrides at runtime. For this reason, the values can be `%env(...)%` placeholders.
Symfony resolves them at runtime, not at the time that it parses the configuration.

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

The bundle applies `dbname`, `host`, `port`, `user` and `password` first, and `dbname_suffix` last. For example,
`dbname: 'foo'` and `dbname_suffix: '_test'` give `foo_test`. A connection with only `dsn` works as before.

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

To get distributed tracing, query logging, and metrics for a connection, enable telemetry on that connection.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

      telemetry:
        service_id: "flow.telemetry"         # Required: Telemetry service ID
        clock_service_id: null               # Optional: PSR-20 clock service (defaults to SystemClock)
        trace_queries: true                  # Record query execution in traces
        transaction_spans: grouped           # grouped | per_operation | off
        collect_metrics: true                # Collect query metrics (duration, row count)
        log_queries: false                   # Log all queries
        max_query_length: 1000               # Truncate queries longer than this (chars)
        include_parameters: false            # Include query parameters in traces
        max_parameters: 10                   # Max number of parameters to include
        max_parameter_length: 100            # Max length of each parameter value (chars)
```

### Web Profiler

This configuration adds a Flow PostgreSQL panel to the Symfony Web Profiler. The panel lists the SQL that the current
request runs. It is similar to the Queries panel of the Doctrine bundle, but it is for the Flow PostgreSQL client and
does not need telemetry. The bundle wraps the client of each connection in a recording wrapper. The wrapper records the
statement, the bound parameters, the duration, the returned or affected rows, the calling code location, and failures.

```yaml
flow_postgresql:
  profiler:
    enabled: ~
    include_parameters: true # show bound query parameters in the panel
    max_queries: 1000            # retained queries; oldest dropped first
    max_retained_parameters: 100 # queries binding more than this keep no parameters
    max_query_length: 1000       # retained statements longer than this are truncated
    migrations: true         # show the Flow Migrations panel (requires migrations enabled)
```

The log has limits. The panel keeps at most `max_queries` entries. After it drops older entries, it shows a note.
Bound parameters use most of the memory. For this reason, a query that binds more than `max_retained_parameters`
values keeps none of them. The panel shows "parameters omitted" for such a query, and you cannot run EXPLAIN on it
from the panel.

This all-or-nothing rule is intentional. It is also the reason that this option has a different name from the
per-connection `telemetry.max_parameters`, which keeps the first N values. The panel binds the retained parameters
again to run EXPLAIN, and a partial list does not make a valid query.

The `max_query_length` option limits the length of the statements. A batch `INSERT` with a thousand `VALUES` tuples
is over 100 KB of SQL. Uncut, `max_queries` of these statements use hundreds of megabytes. The panel marks truncated
statements, and you cannot run EXPLAIN on them from the panel.

Recording is for development only and adds nothing in production. If the profiler is disabled, the bundle wraps no
connection. If WebProfilerBundle is not registered in `enabled: ~` mode, the result is the same. If the profiler is on, you
can turn off recording for a single connection:

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'
      profiler: false # do not record queries in selected connection (default: true)
```

Recording follows `kernel.debug`, like `dbal.profiling` in DoctrineBundle. With `enabled: ~`, the bundle adds the
panel in one case only. WebProfilerBundle must be registered, and the kernel must run in debug mode. For this reason,
`bin/console --no-debug` wraps no connection and records nothing. That command compiles a separate container.
With `enabled: true`, recording is always on, in debug mode and outside it.

If `migrations` are enabled, a separate Flow Migrations panel shows the migrations of the migrations connection. It
lists the executed, pending, and unavailable migrations, with their execution time. It is similar to the panel of the
Doctrine Migrations bundle. The panel queries the database on every profiled request. To disable it and keep the query
panel, set `profiler.migrations: false`:

```yaml
flow_postgresql:
  profiler:
    migrations: false # do not query/show migration status in the profiler (default: true)
```

### Migrations

Migrations are configured at the top level, not per connection. They always run against one connection. To choose
that connection, set `connection`. If you do not set it, the bundle uses the default (first) connection. Every
migration command operates on that connection. There is no connection override per command.

```yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  migrations:
    enabled: true
    connection: ~                                  # Connection migrations run against (default: the first/default connection)
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

To add `IF EXISTS` to every `DROP` that the diff generates, set `drop_if_exists: true`. Use it for convergence
migrations, that is, migrations that run against both fresh and legacy databases. The default is `false`, so a
missing object makes the migration fail (drift detection).
The [`--drop-if-exists`](#generating-migrations-from-schema-diff) flag overrides this key for a single run.

#### Accessing the service container in a migration

If migrations are enabled, the bundle puts the service container of the application into the migration context under
`FlowPostgreSqlBundle::SERVICE_CONTAINER`. Migrations can then read resolved parameters and use any service. Resolved
parameters include values that Symfony resolves at container build time, for example decrypted secrets and
`%env(...)%` processors. This matches the behavior of the container-aware migrations in Doctrine.

Read the container with `MigrationContext::attribute()`, as the example below shows. If the attribute is not set, this
method throws an exception. To make sure that it is set first, use `hasAttribute()`.

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

If you inject the whole container, every service that a migration needs must be public. To avoid this, declare extra
attributes under `migrations.context`. The bundle merges them into the migration context next to the container. Each
value can be a literal, an `@service_id` reference, a `%parameter%` placeholder, or a `%env(VAR)%` expression. To
keep a literal leading `@`, use `@@`. Referenced services can be private.

```yaml
flow_postgresql:
  migrations:
    enabled: true
    context:
      report_generator: '@app.report_generator'        # service (may be private)
      readonly_url: '%env(DATABASE_READONLY_URL)%'      # resolved env
      batch_size: 500                                   # literal
```

A migration reads each key with `MigrationContext::attribute()`:

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

The `service_container` key is always present. A `context` entry cannot override it.

### Catalog Providers

Catalog providers define the target database schema. When you run `flow:migrations:diff`, the bundle compares the
current database state to the catalog. Then it generates migration SQL that removes the differences.

There are three ways to define catalog providers.

#### Attribute-Based Discovery

Add `#[AsCatalogProvider]` to a class that implements `CatalogProvider`, and the bundle finds it automatically:

```php
<?php

namespace App\Database;

use Flow\Bridge\Symfony\PostgreSqlBundle\Attribute\AsCatalogProvider;
use Flow\PostgreSql\Schema\Catalog;
use Flow\PostgreSql\Schema\CatalogProvider;

use function Flow\PostgreSql\DSL\{schema, schema_column_integer, schema_column_text, schema_table};

#[AsCatalogProvider]
final class UsersCatalogProvider implements CatalogProvider
{
    public function get(): Catalog
    {
        return new Catalog([
            schema('public', [
                schema_table('users', [
                    schema_column_integer('id', nullable: false),
                    schema_column_text('email'),
                ]),
            ]),
        ]);
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

Define the catalog directly in the configuration:

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

You can combine multiple catalog providers. `ChainCatalogProvider` merges them into one catalog.

### Exclusions

By default, `flow:migrations:diff` compares every object in the database with the catalog. If no catalog provider
describes an object, the diff reports it as a difference to drop. This is a problem for objects that are created
outside of migrations. Examples are tables that the application generates from content that users upload, or a whole
schema that another system owns. List these objects under `migrations.exclude`, and the diff skips them. The diff
always excludes the table that tracks the migrations.

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

Each entry defines exactly one matcher:

| Key           | Excludes                                                                               |
|---------------|----------------------------------------------------------------------------------------|
| `schema`      | The named schema and every object inside it (tables, views, sequences, functions, ...) |
| `table`       | A table by exact name (shorthand for `exact` narrowed to tables)                       |
| `exact`       | Any object whose name matches exactly                                                  |
| `starts_with` | Any object whose name starts with the given prefix                                     |
| `ends_with`   | Any object whose name ends with the given suffix                                       |
| `pattern`     | Any object whose name matches the given PCRE pattern (with delimiters)                 |
| `policy_id`   | Delegates to a service implementing `Flow\PostgreSql\Schema\Exclusion\ExclusionPolicy` |

You can narrow the `exact`, `starts_with`, `ends_with`, `pattern` and `policy_id` matchers with these scopes:

- `type`: limits the matcher to one object type, which is `table`, `view`, `materialized_view`, `sequence`,
  `function`, `procedure`, `domain` or `extension`. If you do not set it, the matcher applies to every type.
- `for_schema`: limits the matcher to one schema. If you do not set it, the matcher applies to every schema.

The diff filters tables and whole schemas before it reads their structure. If you exclude dynamically created
tables, the diff thus does not read their columns, indexes and constraints on every run.

A custom `policy_id` service implements the same interface. It receives every candidate object:

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

These commands are always available, with or without a migration configuration.

| Command                | Description                                       |
|------------------------|---------------------------------------------------|
| `flow:database:create` | Create the configured database                    |
| `flow:database:drop`   | Drop the configured database (requires `--force`) |
| `flow:sql:run`         | Execute SQL directly on the database              |

The commands above accept `--connection` (`-c`) to target a specific connection. Migration commands do not accept
it. They always run against the configured migrations connection.

### Migration Commands

If the top-level `migrations.enabled: true` is set, these commands are available.

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
| `flow:migrations:up-to-date` | Make sure that all migrations are executed                    |

Common options:

| Option                | Description                                           |
|-----------------------|-------------------------------------------------------|
| `--dry-run`           | Preview changes without applying (migrate, execute)   |
| `--all-or-nothing`    | Wrap all migrations in a single transaction (migrate) |
| `--up` / `--down`     | Migration direction (execute)                         |
| `--allow-empty-diff`  | Do not fail on an empty diff (diff)                   |
| `--from-empty-schema` | Treat the database as empty (diff)                    |
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

Each directory name follows the pattern `{version}` or `{version}_{name}`. The version is a numeric timestamp, for
example `20260401120000`. The name is an optional description.

### Generating Migrations from Schema Diff

This command compares the current database to your catalog providers and generates the migration SQL:

```bash
php bin/console flow:migrations:diff
```

The command creates a new version directory. It contains `migration.php` with the SQL that brings the database in sync
with the catalog. If `generate_rollback` is enabled, it also contains `rollback.php` with the reverse operations.

To generate a migration that assumes an empty database, use `--from-empty-schema`. This helps for the initial
setup.

To add `IF EXISTS` to every generated `DROP` for one run, use `--drop-if-exists`. It overrides the
[`drop_if_exists`](#drop_if_exists) configuration key for that run:

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

Preview the changes and apply nothing:

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

Each connection gets its own set of services (`flow.postgresql.{name}.*`). The bundle aliases the first connection to
the base interfaces (`Client`, `ConnectionParameters`, `Migrator`, and others). Thus you can inject these interfaces
by type-hint and give no connection name.

If migrations are enabled, the bundle registers the migration services once, for `migrations.connection`, or for the
first connection without it. Migration commands always use that connection. Only the database create/drop and
run-sql commands accept `--connection`:

```bash
php bin/console flow:migrations:migrate
```

## Symfony Messenger Integration

The bundle works
with [flow-php/symfony-postgresql-messenger-bridge](/documentation/components/bridges/symfony-postgresql-messenger-bridge.md).
The bridge gives a Symfony Messenger transport that uses the native PostgreSQL client of Flow. It does not need
Doctrine DBAL.

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

If `messenger.enabled: true` is set, the bundle registers the `MessengerCatalogProvider` automatically. Thus the
`messenger_messages` table appears in schema diffs together with your other tables from the catalog.

### Configuration Options

| Option              | Default              | Location                                   | Description                                            |
|---------------------|----------------------|--------------------------------------------|--------------------------------------------------------|
| `table_name`        | `messenger_messages` | `flow_postgresql.messenger`                | Table name in the database                             |
| `schema`            | `public`             | `flow_postgresql.messenger`                | Schema owning the table                                |
| `queue_name`        | `default`            | `framework.messenger.transports.*.options` | Queue name for message routing                         |
| `redeliver_timeout` | `3600`               | `framework.messenger.transports.*.options` | Seconds before unacknowledged messages are redelivered |

The [Symfony PostgreSQL Messenger Bridge](/documentation/components/bridges/symfony-postgresql-messenger-bridge.md)
page has the full documentation.

## Symfony Cache Integration

The bundle works
with [flow-php/symfony-postgresql-cache-bridge](/documentation/components/bridges/symfony-postgresql-cache-bridge.md).
The bridge gives PSR-6 and Symfony Cache pools that use the native PostgreSQL client of Flow. It does not need
Doctrine DBAL. The adapter implements `PruneableInterface`, so `cache:pool:prune` works without extra configuration.

The bundle gives each pool the `ConnectionParameters` of the named connection. Each pool opens its own `pg_connect`,
separate from the `Client` services that application code uses. Because of this isolation, `$cache->save(...)` does
not take part in a transaction that the caller holds. A rollback of that transaction does not roll back the cache write.

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

- `flow.postgresql.cache.pool.<name>`: the cache adapter (public).
- `flow.postgresql.cache.pool.<name>.catalog_provider`: has the `flow.postgresql.catalog_provider` tag, so
  `flow:migrations:diff` includes the table.

3. Connect the pools to the Symfony cache framework with `cache.adapter.psr6`:

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

The `cache.adapter.psr6` wrapper is required. Symfony's `CachePoolPass` overwrites the first constructor argument of
any service that is used directly as `adapter:`. This conflicts with the strict `Client` type of argument 0 in this
bridge.

4. Generate and run the migration to create the cache tables:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

### Configuration Options (per pool)

| Option                                              | Default                                                 | Description                                                                                                           |
|-----------------------------------------------------|---------------------------------------------------------|-----------------------------------------------------------------------------------------------------------------------|
| `connection`                                        | first connection                                        | `flow_postgresql.connections` key the pool uses                                                                       |
| `table_name`                                        | `cache_items`                                           | Table storing pool entries                                                                                            |
| `schema`                                            | `public`                                                | Schema owning the table                                                                                               |
| `id_col` / `data_col` / `lifetime_col` / `time_col` | `item_id` / `item_data` / `item_lifetime` / `item_time` | Column overrides                                                                                                      |
| `namespace`                                         | `''`                                                    | Cache pool namespace. Only characters in `[-+.A-Za-z0-9]`                                                             |
| `default_lifetime`                                  | `0`                                                     | Default TTL in seconds. `0` means no expiry                                                                           |
| `marshaller_service_id`                             | `null`                                                  | Service ID of a custom `MarshallerInterface`                                                                          |
| `share_connection`                                  | `false`                                                 | Reuse the named connection's `Client` instead of opening a dedicated `pg_connect`. See "Sharing the Connection" below |

### Sharing the Connection

By default, each pool opens its own `pg_connect` from the parameters of the named connection. Because of this
isolation, a failed transaction of the caller does not roll back `$cache->save(...)`.

To reuse the existing `flow.postgresql.<connection>.client` service, set `share_connection: true`. This has a benefit
and a cost:

- Pro: one `pg_connect` less per worker per pool. If the connection budget is small, this helps.
- Con: if the calling code is inside a transaction, cache writes take part in it. A rollback on the caller's
  transaction also rolls back the cache write.

Use `share_connection: true` only for an explicit reason to share state. Examples are a transactional outbox, or cache
writes that belong to the unit of work of the caller.

### Pruning

To remove expired rows, run the standard Symfony command from a cron schedule:

```bash
php bin/console cache:pool:prune
```

Without pruning, expired rows stay in the table. Reads filter them out. Two events delete them: a new fetch of the same key, or
a run of `cache:pool:prune`.

The [Symfony PostgreSQL Cache Bridge](/documentation/components/bridges/symfony-postgresql-cache-bridge.md) page has
the full documentation.

## Symfony Session Integration

The bundle works
with [flow-php/symfony-postgresql-session-bridge](/documentation/components/bridges/symfony-postgresql-session-bridge.md).
The bridge gives a Symfony session handler that uses the native PostgreSQL client of Flow. It does not need PDO or
Doctrine DBAL. The table layout is byte-compatible with `PdoSessionHandler`, so you can reuse existing session rows as
they are.

The bundle gives the handler the `ConnectionParameters` of the named connection. The handler opens its own
`pg_connect`, separate from the `Client` services that application code uses. For this reason, `LOCK_TRANSACTIONAL` is
safe by default. The session transaction is on a dedicated connection and never contains unrelated request work.

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

If you enable this section, the bundle registers three services:

- `flow.postgresql.session.handler`: the `FlowPostgreSqlSessionHandler` (public, also aliased to
  `\SessionHandlerInterface`).
- `flow.postgresql.session.catalog_provider`: has the `flow.postgresql.catalog_provider` tag, so
  `flow:migrations:diff` includes the `sessions` table.
- `flow.postgresql.session.purge_command`: the command that [Purging Sessions](#purging-sessions) describes.

3. Connect the handler to the Symfony session framework:

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

| Option             | Default          | Description                                                                                                           |
|--------------------|------------------|-----------------------------------------------------------------------------------------------------------------------|
| `enabled`          | `false`          | Turns the session integration on or off                                                                               |
| `connection`       | first connection | `flow_postgresql.connections` key the handler uses                                                                    |
| `table_name`       | `sessions`       | Table storing sessions                                                                                                |
| `schema`           | `public`         | Schema owning the table                                                                                               |
| `id_col`           | `sess_id`        | Column override                                                                                                       |
| `data_col`         | `sess_data`      | Column override                                                                                                       |
| `lifetime_col`     | `sess_lifetime`  | Column override                                                                                                       |
| `time_col`         | `sess_time`      | Column override                                                                                                       |
| `lock_mode`        | `transactional`  | One of `none`, `advisory`, `transactional`. The bridge documentation describes the trade-offs                         |
| `ttl`              | `null`           | Session lifetime in seconds. With `null`, the ini value `session.gc_maxlifetime` applies                              |
| `share_connection` | `false`          | Reuse the named connection's `Client` instead of opening a dedicated `pg_connect`. See "Sharing the Connection" below |

### Sharing the Connection

By default, the handler opens its own `pg_connect` from the parameters of the named connection. Because of this
isolation, the transactions and locks of the session handler stay off the connection that application code uses.

To reuse the existing `flow.postgresql.<connection>.client` service, set `share_connection: true`. Use it only for an
explicit reason. Examples are a small connection budget, or a need to keep session state on the same connection as
application code.

### Purging Sessions

The probabilistic garbage collection of PHP (`session.gc_probability`) already removes expired rows at the end of a
request. Most applications thus need no extra configuration. For predictable cleanup from a cron schedule, the bundle
has this command:

```bash
# Delete only sessions whose sess_lifetime is in the past (default behavior)
php bin/console flow:postgresql:session:purge

# Or explicitly:
php bin/console flow:postgresql:session:purge --expired

# Wipe every session (logs everyone out - destructive)
php bin/console flow:postgresql:session:purge --all
```

You cannot use `--expired` and `--all` together. If you use both, the command returns a non-zero exit code.

The [Symfony PostgreSQL Session Bridge](/documentation/components/bridges/symfony-postgresql-session-bridge.md) page
has the full documentation.

## Test Transaction Rollback

The bundle works
with [flow-php/phpunit-postgresql-bridge](/documentation/components/bridges/phpunit-postgresql-bridge.md). The bridge
wraps each PHPUnit test in a database transaction and rolls it back after the test. Thus your test database stays clean
without manual teardown.

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

3. Enable transaction rollback per connection in a configuration for the test environment:

```yaml
# config/packages/test/flow_postgresql.yaml
flow_postgresql:
  connections:
    default:
      test_transaction_rollback: true
    readonly:
      test_transaction_rollback: false  # default, no wrapping
```

If `test_transaction_rollback` is `true`, the bundle replaces `PgSqlClient::connect` with `StaticClient::connect` for
that connection. Thus the same `Client` instance is reused across kernel reboots in one test. The transaction that the
PHPUnit extension starts then covers all database operations of your services.

### How It Works

1. PHPUnit starts. The extension enables `StaticClient` caching.
2. Before each test, the extension rolls back the previous transaction and begins a new one.
3. The Symfony kernel boots. The bundle creates the client with `StaticClient::connect()`, which returns the cached
   instance with an active transaction.
4. The test runs. All queries go through the same cached client, inside the transaction.
5. The next test starts. The extension rolls back all changes from the previous test.

PostgreSQL supports transactional DDL, so the rollback also undoes `CREATE TABLE` and `ALTER TABLE` statements.

### Skipping Rollback

To opt out for specific tests, classes, or abstract parent classes, use the `#[SkipTransactionRollback]` attribute.
The [PHPUnit PostgreSQL Bridge documentation](/documentation/components/bridges/phpunit-postgresql-bridge.md) has the
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
        transaction_spans: grouped
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
