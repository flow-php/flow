# Symfony PostgreSQL Bundle

Symfony bundle integrating Flow PHP's PostgreSQL library with Symfony applications, providing database management,
schema migrations, catalog-driven schema diffing, and optional telemetry for query tracing and metrics.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-bundle)
- [Installation](/documentation/installation/packages/symfony-postgresql-bundle.md)
- [GitHub](https://github.com/flow-php/symfony-postgresql-bundle)

[TOC]

## Installation

For detailed installation instructions, see the [installation page](/documentation/installation/packages/symfony-postgresql-bundle.md).

## Overview

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
```

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

## Console Commands

### Database Commands

These commands are always available, regardless of migration configuration.

| Command | Description |
|---------|-------------|
| `flow:database:create` | Create the configured database |
| `flow:database:drop` | Drop the configured database (requires `--force`) |
| `flow:sql:run` | Execute SQL directly on the database |

All commands accept `--connection` (`-c`) to target a specific connection.

### Migration Commands

These commands are available when `migrations.enabled: true` for at least one connection.

| Command | Description |
|---------|-------------|
| `flow:migrations:diff` | Generate a migration by comparing the database to the catalog |
| `flow:migrations:generate` | Generate a blank migration |
| `flow:migrations:migrate` | Execute pending migrations |
| `flow:migrations:execute` | Execute a single migration version |
| `flow:migrations:status` | View migration status |
| `flow:migrations:current` | Output the current migration version |
| `flow:migrations:latest` | Output the latest available migrations |
| `flow:migrations:list` | List all available migrations |
| `flow:migrations:up-to-date` | Check if all migrations have been executed |

**Common options:**

| Option | Description |
|--------|-------------|
| `--connection` (`-c`) | Target a specific connection |
| `--dry-run` | Preview changes without applying (migrate, execute) |
| `--all-or-nothing` | Wrap all migrations in a single transaction (migrate) |
| `--up` / `--down` | Migration direction (execute) |
| `--allow-empty-diff` | Don't fail when no changes detected (diff) |
| `--from-empty-schema` | Generate as if the database were empty (diff) |

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
