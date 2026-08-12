# Client Connection

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The PostgreSQL client uses the `ext-pgsql` extension for database connectivity. Connection parameters can be specified
using connection strings, DSN URLs, or individual parameters.

## Connection String

The most common approach using libpq-style connection strings:

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

## DSN URL

Parse standard PostgreSQL DSN format commonly used in environment variables:

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

`ConnectionParameters` is immutable. Each `with*()` method returns a new instance, so a parsed DSN
can be adjusted without re-parsing. 

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

`withDatabaseSuffix()` appends a suffix to the configured database name. The base DSN stays constant
and only the suffix changes per environment, which makes per-worker parallel-test databases work
(`mydb` → `mydb_test7`) without a second DSN. The suffix is appended verbatim - nothing is inserted
automatically, so include a separator yourself if you want one (e.g. `_test7`). An empty suffix is a
no-op:

```php
<?php

use Flow\PostgreSql\Client\DsnParser;

$params = (new DsnParser())->parse('postgresql://user:pass@localhost:5432/mydb')
    ->withDatabaseSuffix('_test7'); // database() === 'mydb_test7'
```

When combined with `withDatabase()`, apply the override first - the suffix is appended to the
overridden name (`withDatabase('other')->withDatabaseSuffix('_test')` ⇒ `other_test`).

## Connection Lifecycle

### Checking Connection Status

```php
<?php

if ($client->isConnected()) {
    // Connection is alive
}
```

### Closing Connections

Always close connections when done:

```php
<?php

$client->close();
```

For long-running applications, consider connection pooling at the infrastructure level (e.g., PgBouncer).

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

For the full list of libpq connection parameters, see
the [PostgreSQL documentation](https://www.postgresql.org/docs/current/libpq-connect.html#LIBPQ-PARAMKEYWORDS).
