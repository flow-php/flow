# Client Connection

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The PostgreSQL client uses the `ext-pgsql` extension to connect to the database. You can give the connection
parameters as connection strings, DSN URLs, or individual parameters.

## Connection String

The most common approach uses libpq-style connection strings:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection};

// Basic connection
$client = pgsql_client(
    pgsql_connection('host=localhost dbname=mydb user=postgres password=secret')
);

// With additional options
$client = pgsql_client(
    pgsql_connection('host=localhost port=5432 dbname=mydb user=postgres sslmode=require')
);

// Unix socket connection
$client = pgsql_client(
    pgsql_connection('host=/var/run/postgresql dbname=mydb user=postgres')
);
```

Put a value with spaces, quotes or backslashes in single quotes:

```php
<?php

use function Flow\PostgreSql\DSL\pgsql_connection;

$params = pgsql_connection("host=localhost dbname=mydb password='my secret'");
```

## DSN URL

Parse the standard PostgreSQL DSN format that environment variables often use:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

// Standard PostgreSQL DSN
$client = pgsql_client(
    pgsql_connection_dsn('postgres://myuser:secret@localhost:5432/mydb')
);

// With SSL mode
$client = pgsql_client(
    pgsql_connection_dsn('postgresql://user:pass@db.example.com/app?sslmode=require')
);

// Symfony/Doctrine format
$client = pgsql_client(
    pgsql_connection_dsn('pgsql://user:pass@localhost/mydb')
);

// From environment variable
$client = pgsql_client(
    pgsql_connection_dsn(getenv('DATABASE_URL'))
);
```

Supported schemes: `postgres://`, `postgresql://`, `pgsql://`

## Explicit Parameters

For better type safety and IDE support:

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_params};

$client = pgsql_client(
    pgsql_connection_params(
        database: 'mydb',
        host: 'localhost',
        port: 5432,
        user: 'myuser',
        password: 'secret',
    )
);

// With additional libpq options
$client = pgsql_client(
    pgsql_connection_params(
        database: 'mydb',
        host: 'db.example.com',
        user: 'myuser',
        password: 'secret',
        options: [
            'sslmode' => 'require',
            'connect_timeout' => '10',
        ],
    )
);
```

## Overriding Parsed Parameters

`ConnectionParameters` is immutable. Each `with*()` method returns a new instance, so you can change a parsed DSN
without a new parse.

```php
<?php

use Flow\PostgreSql\Client\DsnParser;

$params = (new DsnParser())->parse('postgresql://user:pass@localhost:5432/mydb')
    ->withHost('db.internal')
    ->withPort(5544)
    ->withUser('svc')
    ->withPassword('secret')
    ->withDatabase('analytics');
```

### Database Suffix

`withDatabaseSuffix()` appends a suffix to the configured database name. The base DSN stays the same, and only the
suffix changes for each environment. Parallel-test databases for each worker then work without a second DSN
(`mydb` → `mydb_test7`). The suffix is appended exactly as it is. Nothing is inserted automatically, so add a
separator yourself, for example `_test7`. An empty suffix is a no-op:

```php
<?php

use Flow\PostgreSql\Client\DsnParser;

$params = (new DsnParser())->parse('postgresql://user:pass@localhost:5432/mydb')
    ->withDatabaseSuffix('_test7'); // database() === 'mydb_test7'
```

With `withDatabase()`, apply the override first. The suffix is then appended to the overridden name
(`withDatabase('other')->withDatabaseSuffix('_test')` ⇒ `other_test`).

## Session Settings

```php
<?php

use function Flow\PostgreSql\DSL\{pgsql_client, pgsql_connection_dsn};

$client = pgsql_client(
    pgsql_connection_dsn('postgresql://user:pass@localhost:5432/mydb')
        ->withSessionSettings([
            'statement_timeout' => '30s',
            'lock_timeout' => '10s',
            'idle_in_transaction_session_timeout' => '60s',
        ])
);
```

Each pair goes to the server as `-c name=value` in the libpq `options` key, after a value that is already there. A
name holds letters, digits, `_` and `.`. The client escapes spaces and backslashes in the value.

### PgBouncer

PgBouncer rejects a connection that sends startup parameters that it does not track. With
[`ignore_startup_parameters`](https://www.pgbouncer.org/config.html#ignore_startup_parameters) it drops them, so the
server does not apply the settings. Behind PgBouncer, set them with `SET` after you connect, or on the role with
`ALTER ROLE ... SET`.

## Connection Lifecycle

### Checking Connection Status

```php
<?php

if ($client->isConnected()) {
    // Connection is alive
}
```

### Closing Connections

Always close connections at the end:

```php
<?php

$client->close();
```

For long-running applications, think about connection pooling at the infrastructure level, for example PgBouncer.

## Connection Parameters Reference

| Parameter             | Description                                                        | Default          |
|-----------------------|--------------------------------------------------------------------|------------------|
| `host`                | Server hostname or IP address                                      | `localhost`      |
| `port`                | Server port                                                        | `5432`           |
| `dbname` / `database` | Database name                                                      | (required)       |
| `user`                | Username                                                           | (optional)       |
| `password`            | Password                                                           | (optional)       |
| `sslmode`             | SSL mode (disable, allow, prefer, require, verify-ca, verify-full) | `prefer`         |
| `connect_timeout`     | Connection timeout in seconds                                      | (system default) |
| `application_name`    | Application name for pg_stat_activity                              | (none)           |

`ConnectionParameters::OPTION_KEYS` lists the libpq keys that a DSN query string and the Symfony bundle accept.

For the full list of libpq connection parameters, read
the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).
