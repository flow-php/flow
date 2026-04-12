# Symfony PostgreSQL Messenger Bridge

A Symfony Messenger transport backed by Flow PHP's native PostgreSQL library. Replaces `symfony/doctrine-messenger` without requiring Doctrine DBAL — messages are stored directly in PostgreSQL using Flow's query builder and client.

- [Back](/documentation/introduction.md)
- [Packagist](https://packagist.org/packages/flow-php/symfony-postgresql-messenger-bridge)
- [GitHub](https://github.com/flow-php/symfony-postgresql-messenger-bridge)
- [API Reference](/documentation/api/bridge/symfony-postgresql-messenger)

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-messenger-bridge:~--FLOW_PHP_VERSION--
```

This package is used together with the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md), which registers the transport factory and catalog provider automatically.

## How It Works

The bridge provides a Symfony Messenger transport that stores messages in a PostgreSQL table using Flow's native `Client`. No Doctrine DBAL is involved.

- **Sending** inserts a row with `body`, `headers`, `queue_name`, `created_at`, and `available_at`
- **Receiving** uses `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction for safe concurrent consumption
- **Delayed messages** offset `available_at` by the `DelayStamp` value (in milliseconds)
- **Redelivery** of timed-out messages is handled automatically based on `redeliver_timeout`
- **Keepalive** refreshes `delivered_at` to prevent redelivery during long-running handlers

## Configuration

### 1. Enable Messenger

In your `flow_postgresql` configuration, enable messenger at the root level:

```yaml
# config/packages/flow_postgresql.yaml
flow_postgresql:
  connections:
    default:
      dsn: '%env(DATABASE_URL)%'

  messenger:
    enabled: true
    table_name: messenger_messages  # default
    schema: public                  # default
```

This registers a `MessengerCatalogProvider` and a transport factory that can resolve any configured connection by name. The `messenger_messages` table will be included in schema diffs and migrations automatically.

### 2. Configure the Messenger Transport

```yaml
# config/packages/messenger.yaml
framework:
  messenger:
    transports:
      async:
        dsn: 'flow-pgsql://default'
        options:
          queue_name: default           # default
          redeliver_timeout: 3600       # seconds, default
    routing:
      App\Message\MyMessage: async
```

The DSN format is `flow-pgsql://<connection_name>` where `<connection_name>` matches a connection defined under `flow_postgresql.connections`. Both `flow-pgsql://` and `flow-postgresql://` schemes are supported.

### 3. Create the Table via Migrations

Because the catalog provider is registered automatically, the messenger table appears in migration diffs:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

This is the recommended approach. The bridge does **not** implement `SetupableTransportInterface` — there is no `messenger:setup-transports` support. Use migrations for schema management.

## DSN Options

Options can be set in the DSN query string or in the `options` array under the transport configuration. The `options` array takes precedence over DSN query parameters.

| Option | Default | Description |
|--------|---------|-------------|
| `queue_name` | `default` | Queue name for message routing |
| `table_name` | `messenger_messages` | Table storing messages (must match `flow_postgresql.messenger` config) |
| `schema` | `public` | Schema owning the table (must match `flow_postgresql.messenger` config) |
| `redeliver_timeout` | `3600` | Seconds before a delivered-but-unacknowledged message becomes eligible for redelivery |

Example with DSN options:

```yaml
framework:
  messenger:
    transports:
      high_priority:
        dsn: 'flow-pgsql://default?queue_name=high&redeliver_timeout=1800'
```

## Multiple Queues

Multiple transports can share the same table but use different queue names:

```yaml
framework:
  messenger:
    transports:
      async:
        dsn: 'flow-pgsql://default'
        options:
          queue_name: default
      high_priority:
        dsn: 'flow-pgsql://default'
        options:
          queue_name: high
    routing:
      App\Message\ImportantMessage: high_priority
      App\Message\BackgroundTask: async
```

## Table Schema

The messenger table has the following structure:

| Column | Type | Description |
|--------|------|-------------|
| `id` | `bigint GENERATED ALWAYS AS IDENTITY` | Primary key |
| `body` | `text` | Serialized message body |
| `headers` | `text` | JSON-encoded message headers (stamps, type info) |
| `queue_name` | `varchar(190)` | Queue name for message routing |
| `created_at` | `timestamptz` | When the message was dispatched |
| `available_at` | `timestamptz` | When the message becomes available for consumption |
| `delivered_at` | `timestamptz` | When a worker picked up the message (null = not yet delivered) |

Three indexes are created: on `queue_name`, `available_at`, and `delivered_at`.
