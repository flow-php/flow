# Copy Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Copy Query Builder provides a fluent, type-safe interface for constructing PostgreSQL COPY statements. It supports both COPY TO (data export) and COPY FROM (data import) operations with files, programs, STDIN, and STDOUT.

## COPY FROM (Data Import)

### Basic COPY FROM

```php
<?php

use function Flow\PgQuery\DSL\copy_from;

$query = copy_from()
    ->table('users')
    ->fromFile('/tmp/users.csv');

echo $query->toSQL();
// COPY users FROM '/tmp/users.csv'
```

### COPY FROM with Columns

```php
<?php

use function Flow\PgQuery\DSL\copy_from;

$query = copy_from()
    ->table('users', 'id', 'name', 'email')
    ->fromFile('/tmp/users.csv');

echo $query->toSQL();
// COPY users(id, name, email) FROM '/tmp/users.csv'
```

### COPY FROM STDIN

```php
<?php

use function Flow\PgQuery\DSL\copy_from;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_from()
    ->table('users')
    ->fromStdin()
    ->format(CopyFormat::CSV);

echo $query->toSQL();
// COPY users FROM STDIN WITH (format csv)
```

### COPY FROM PROGRAM

```php
<?php

use function Flow\PgQuery\DSL\copy_from;

$query = copy_from()
    ->table('logs')
    ->fromProgram('gunzip -c /var/log/app.log.gz');

echo $query->toSQL();
// COPY logs FROM PROGRAM 'gunzip -c /var/log/app.log.gz'
```

### CSV Format with Options

```php
<?php

use function Flow\PgQuery\DSL\copy_from;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_from()
    ->table('data')
    ->fromFile('/tmp/data.csv')
    ->format(CopyFormat::CSV)
    ->withHeader()
    ->delimiter(';')
    ->nullAs('NULL')
    ->quote("'")
    ->escape('\\')
    ->encoding('UTF8');

echo $query->toSQL();
// COPY data FROM '/tmp/data.csv' WITH (format csv, delimiter ';', null 'NULL', header true, quote '''', escape '\\', encoding 'UTF8')
```

### Force Not Null

Treat specified columns as non-nullable during import:

```php
<?php

use function Flow\PgQuery\DSL\copy_from;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_from()
    ->table('users')
    ->fromFile('/tmp/users.csv')
    ->format(CopyFormat::CSV)
    ->forceNotNull('name', 'email');

echo $query->toSQL();
// COPY users FROM '/tmp/users.csv' WITH (format csv, force_not_null (name, email))
```

### Force Null

Treat specified values as NULL for these columns:

```php
<?php

use function Flow\PgQuery\DSL\copy_from;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_from()
    ->table('users')
    ->fromFile('/tmp/users.csv')
    ->format(CopyFormat::CSV)
    ->forceNull('description', 'notes');

echo $query->toSQL();
// COPY users FROM '/tmp/users.csv' WITH (format csv, force_null (description, notes))
```

### Error Handling

Control behavior when encountering invalid data:

```php
<?php

use function Flow\PgQuery\DSL\copy_from;
use Flow\PgQuery\QueryBuilder\Copy\CopyOnError;

$query = copy_from()
    ->table('events')
    ->fromFile('/tmp/events.csv')
    ->onError(CopyOnError::IGNORE);

echo $query->toSQL();
// COPY events FROM '/tmp/events.csv' WITH (on_error 'ignore')
```

## COPY TO (Data Export)

### Basic COPY TO

```php
<?php

use function Flow\PgQuery\DSL\copy_to;

$query = copy_to()
    ->table('users')
    ->toFile('/tmp/users.csv');

echo $query->toSQL();
// COPY users TO '/tmp/users.csv'
```

### COPY TO with Columns

```php
<?php

use function Flow\PgQuery\DSL\copy_to;

$query = copy_to()
    ->table('users', 'id', 'name', 'email')
    ->toFile('/tmp/users.csv');

echo $query->toSQL();
// COPY users(id, name, email) TO '/tmp/users.csv'
```

### COPY TO STDOUT

```php
<?php

use function Flow\PgQuery\DSL\copy_to;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_to()
    ->table('users')
    ->toStdout()
    ->format(CopyFormat::CSV);

echo $query->toSQL();
// COPY users TO STDOUT WITH (format csv)
```

### COPY TO PROGRAM

```php
<?php

use function Flow\PgQuery\DSL\copy_to;

$query = copy_to()
    ->table('logs')
    ->toProgram('gzip > /tmp/logs.csv.gz');

echo $query->toSQL();
// COPY logs TO PROGRAM 'gzip > /tmp/logs.csv.gz'
```

### COPY with SELECT Query

Export results of a query instead of a table:

```php
<?php

use function Flow\PgQuery\DSL\{copy_to, select, col, table};
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = copy_to()
    ->query($selectQuery)
    ->toFile('/tmp/active_users.csv')
    ->format(CopyFormat::CSV);

echo $query->toSQL();
// COPY (SELECT id, name FROM users) TO '/tmp/active_users.csv' WITH (format csv)
```

### Binary Format

```php
<?php

use function Flow\PgQuery\DSL\copy_to;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

$query = copy_to()
    ->table('data')
    ->toFile('/tmp/data.bin')
    ->format(CopyFormat::BINARY);

echo $query->toSQL();
// COPY data TO '/tmp/data.bin' WITH (format binary)
```

### Force Quote

Quote specific columns or all columns in CSV output:

```php
<?php

use function Flow\PgQuery\DSL\copy_to;
use Flow\PgQuery\QueryBuilder\Copy\CopyFormat;

// Quote specific columns
$query = copy_to()
    ->table('products')
    ->toFile('/tmp/products.csv')
    ->format(CopyFormat::CSV)
    ->forceQuote('name', 'description');

echo $query->toSQL();
// COPY products TO '/tmp/products.csv' WITH (format csv, force_quote (name, description))

// Quote all columns
$query = copy_to()
    ->table('products')
    ->toFile('/tmp/products.csv')
    ->format(CopyFormat::CSV)
    ->forceQuoteAll();

echo $query->toSQL();
// COPY products TO '/tmp/products.csv' WITH (format csv, force_quote *)
```

## Schema-Qualified Tables

```php
<?php

use function Flow\PgQuery\DSL\{copy_from, copy_to};

$query = copy_from()
    ->table('analytics.events')
    ->fromFile('/tmp/events.csv');

echo $query->toSQL();
// COPY analytics.events FROM '/tmp/events.csv'

$query = copy_to()
    ->table('analytics.events')
    ->toFile('/tmp/events.csv');

echo $query->toSQL();
// COPY analytics.events TO '/tmp/events.csv'
```

## Copy Formats

The `CopyFormat` enum provides three format options:

| Format | Description |
|--------|-------------|
| `CopyFormat::TEXT` | Default PostgreSQL text format |
| `CopyFormat::CSV` | Comma-separated values format |
| `CopyFormat::BINARY` | PostgreSQL binary format |

## Error Handling Options

The `CopyOnError` enum (COPY FROM only) provides error handling options:

| Option | Description |
|--------|-------------|
| `CopyOnError::STOP` | Stop on first error (default) |
| `CopyOnError::IGNORE` | Skip rows with errors and continue |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
