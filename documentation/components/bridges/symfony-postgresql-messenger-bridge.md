---
package: flow-php/symfony-postgresql-messenger-bridge
---

# Symfony PostgreSQL Messenger Bridge

A Symfony Messenger transport on the native PostgreSQL library of Flow PHP. It replaces `symfony/doctrine-messenger` and does not need Doctrine DBAL. Messages are stored directly in PostgreSQL with the query builder and client of Flow.

[PACKAGE_NAV]

[TOC]

## Installation

```bash
composer require flow-php/symfony-postgresql-messenger-bridge:~--FLOW_PHP_VERSION--
```

Use this package together with the [Symfony PostgreSQL Bundle](/documentation/components/bridges/symfony-postgresql-bundle.md). The bundle registers the transport factory and the catalog provider automatically.

## How It Works

The bridge has a Symfony Messenger transport. It stores messages in a PostgreSQL table with the native `Client` of Flow. Doctrine DBAL is not used.

- A send inserts a row with `body`, `headers`, `queue_name`, `created_at`, and `available_at`.
- A receive uses `SELECT ... FOR UPDATE SKIP LOCKED` inside a transaction, so concurrent consumers are safe.
- Delayed messages move `available_at` by the `DelayStamp` value (in milliseconds).
- Timed-out messages are delivered again automatically, after `redeliver_timeout`.
- Keepalive refreshes `delivered_at`, so that long-running handlers do not cause a second delivery.

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

This registers a `MessengerCatalogProvider` and a transport factory that resolves each configured connection by name. Schema diffs and migrations then include the `messenger_messages` table automatically.

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

The DSN format is `flow-pgsql://<connection_name>`. `<connection_name>` is the name of a connection under `flow_postgresql.connections`. The `flow-pgsql://` and `flow-postgresql://` schemes are both supported.

### 3. Create the Table via Migrations

The catalog provider is registered automatically, so the messenger table is in the migration diffs:

```bash
php bin/console flow:migrations:diff
php bin/console flow:migrations:migrate
```

This is the recommended approach. The bridge does not implement `SetupableTransportInterface`, so `messenger:setup-transports` is not supported. Use migrations for schema management.

## DSN Options

You can set options in the DSN query string or in the `options` array of the transport configuration. The `options` array wins over DSN query parameters.

| Option              | Default              | Description                                                                           |
|---------------------|----------------------|---------------------------------------------------------------------------------------|
| `queue_name`        | `default`            | Queue name for message routing                                                        |
| `table_name`        | `messenger_messages` | Table that stores messages (must match the `flow_postgresql.messenger` configuration) |
| `schema`            | `public`             | Schema that owns the table (must match the `flow_postgresql.messenger` configuration) |
| `redeliver_timeout` | `3600`               | Seconds before a delivered but unacknowledged message can be delivered again          |

Example with DSN options:

```yaml
framework:
  messenger:
    transports:
      high_priority:
        dsn: 'flow-pgsql://default?queue_name=high&redeliver_timeout=1800'
```

## Multiple Queues

Multiple transports can share the same table with different queue names:

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

The messenger table has this structure:

| Column         | Type                                  | Description                                                    |
|----------------|---------------------------------------|----------------------------------------------------------------|
| `id`           | `bigint GENERATED ALWAYS AS IDENTITY` | Primary key                                                    |
| `body`         | `text`                                | Serialized message body                                        |
| `headers`      | `text`                                | JSON-encoded message headers (stamps, type info)               |
| `queue_name`   | `varchar(190)`                        | Queue name for message routing                                 |
| `created_at`   | `timestamptz`                         | Time of the dispatch                                           |
| `available_at` | `timestamptz`                         | Time from which consumers can take the message                 |
| `delivered_at` | `timestamptz`                         | Time of the pickup by a worker (null = not delivered yet)      |

The bridge creates three indexes: on `queue_name`, `available_at`, and `delivered_at`.
