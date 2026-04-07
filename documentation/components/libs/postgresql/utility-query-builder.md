# Utility Query Builder

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The Utility Query Builder provides a fluent, type-safe interface for constructing PostgreSQL utility statements. It supports VACUUM, ANALYZE, EXPLAIN, LOCK TABLE, COMMENT, CLUSTER, and DISCARD operations.

## VACUUM

VACUUM reclaims storage occupied by dead tuples and optionally updates planner statistics.

### Basic VACUUM

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum();

echo $query->toSql();
// VACUUM
```

### VACUUM Single Table

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->table('users');

echo $query->toSql();
// VACUUM users
```

### VACUUM Multiple Tables

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->tables('users', 'orders', 'products');

echo $query->toSql();
// VACUUM users, orders, products
```

### VACUUM with Schema

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->table('public.users');

echo $query->toSql();
// VACUUM public.users
```

### VACUUM Specific Columns

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->table('users', 'email', 'name');

echo $query->toSql();
// VACUUM users (email, name)
```

### VACUUM FULL

VACUUM FULL rewrites the entire table to reclaim maximum space:

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->full()->tables('users');

echo $query->toSql();
// VACUUM (FULL) users
```

### VACUUM ANALYZE

VACUUM ANALYZE updates planner statistics after vacuuming:

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;

$query = vacuum()->analyze()->tables('users');

echo $query->toSql();
// VACUUM (ANALYZE) users
```

### VACUUM with Options

```php
<?php

use function Flow\PostgreSql\DSL\vacuum;
use Flow\PostgreSql\QueryBuilder\Utility\IndexCleanup;

// Full vacuum with analyze and verbose output
$query = vacuum()
    ->full()
    ->analyze()
    ->verbose()
    ->table('users');

echo $query->toSql();
// VACUUM (FULL, FREEZE, VERBOSE, ANALYZE) users

// Skip locked tables
$query = vacuum()
    ->skipLocked()
    ->table('users');

echo $query->toSql();
// VACUUM (SKIP_LOCKED) users

// Parallel vacuum
$query = vacuum()
    ->parallel(4)
    ->table('users');

echo $query->toSql();
// VACUUM (PARALLEL 4) users

// Index cleanup options
$query = vacuum()
    ->indexCleanup(IndexCleanup::OFF)
    ->table('users');

echo $query->toSql();
// VACUUM (INDEX_CLEANUP off) users

// Process options
$query = vacuum()
    ->processMain(true)
    ->processToast(false)
    ->table('users');

echo $query->toSql();
// VACUUM (PROCESS_MAIN true, PROCESS_TOAST false) users

// Truncate option
$query = vacuum()
    ->truncate(true)
    ->table('users');

echo $query->toSql();
// VACUUM (TRUNCATE true) users

// Freeze option
$query = vacuum()
    ->freeze()
    ->table('users');

echo $query->toSql();
// VACUUM (FREEZE) users

// Disable page skipping
$query = vacuum()
    ->disablePageSkipping()
    ->table('users');

echo $query->toSql();
// VACUUM (DISABLE_PAGE_SKIPPING) users
```

## ANALYZE

ANALYZE collects statistics about table contents for the query planner.

### Basic ANALYZE

```php
<?php

use function Flow\PostgreSql\DSL\analyze;

$query = analyze();

echo $query->toSql();
// ANALYZE
```

### ANALYZE Single Table

```php
<?php

use function Flow\PostgreSql\DSL\analyze;

$query = analyze()->table('users');

echo $query->toSql();
// ANALYZE users
```

### ANALYZE with Columns

```php
<?php

use function Flow\PostgreSql\DSL\analyze;

$query = analyze()->table('users', 'email', 'name');

echo $query->toSql();
// ANALYZE users (email, name)
```

### ANALYZE Multiple Tables

```php
<?php

use function Flow\PostgreSql\DSL\analyze;

$query = analyze()->tables('users', 'orders', 'products');

echo $query->toSql();
// ANALYZE users, orders, products
```

### ANALYZE with Options

```php
<?php

use function Flow\PostgreSql\DSL\analyze;

// Verbose output
$query = analyze()
    ->verbose()
    ->table('users');

echo $query->toSql();
// ANALYZE (VERBOSE) users

// Skip locked tables
$query = analyze()
    ->skipLocked()
    ->table('users');

echo $query->toSql();
// ANALYZE (SKIP_LOCKED) users
```

## EXPLAIN

EXPLAIN shows the execution plan for a statement.

### Basic EXPLAIN

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};

$query = explain(select()->from('users'));

echo $query->toSql();
// EXPLAIN SELECT * FROM users
```

### EXPLAIN ANALYZE

EXPLAIN ANALYZE actually executes the query and shows real timing:

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};

$query = explain(select()->from('users'))->analyze();

echo $query->toSql();
// EXPLAIN (ANALYZE) SELECT * FROM users
```

### EXPLAIN with Verbose

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};

$query = explain(select()->from('users'))
    ->verbose();

echo $query->toSql();
// EXPLAIN (VERBOSE) SELECT * FROM users
```

### EXPLAIN with Format

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;

// JSON format
$query = explain(select()->from('users'))
    ->format(ExplainFormat::JSON);

echo $query->toSql();
// EXPLAIN (FORMAT json) SELECT * FROM users

// XML format
$query = explain(select()->from('users'))
    ->format(ExplainFormat::XML);

echo $query->toSql();
// EXPLAIN (FORMAT xml) SELECT * FROM users

// YAML format
$query = explain(select()->from('users'))
    ->format(ExplainFormat::YAML);

echo $query->toSql();
// EXPLAIN (FORMAT yaml) SELECT * FROM users
```

### EXPLAIN with Full Options

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};
use Flow\PostgreSql\QueryBuilder\Utility\ExplainFormat;

$query = explain(select()->from('users'))
    ->analyze()
    ->verbose()
    ->buffers(true)
    ->timing(true)
    ->costs(true)
    ->format(ExplainFormat::JSON);

echo $query->toSql();
// EXPLAIN (ANALYZE, VERBOSE, BUFFERS true, TIMING true, COSTS true, FORMAT json) SELECT * FROM users
```

### EXPLAIN with Costs

```php
<?php

use function Flow\PostgreSql\DSL\{explain, select};

// With costs
$query = explain(select()->from('users'))
    ->costs(true);

echo $query->toSql();
// EXPLAIN (COSTS true) SELECT * FROM users

// Without costs
$query = explain(select()->from('users'))
    ->costs(false);

echo $query->toSql();
// EXPLAIN (COSTS false) SELECT * FROM users
```

## LOCK TABLE

LOCK TABLE obtains a table-level lock for the current transaction.

### Basic LOCK

```php
<?php

use function Flow\PostgreSql\DSL\lock_table;

$query = lock_table('users');

echo $query->toSql();
// LOCK TABLE users
```

### LOCK with Mode

```php
<?php

use function Flow\PostgreSql\DSL\lock_table;
use Flow\PostgreSql\QueryBuilder\Utility\LockMode;

// Using mode shortcuts
$query = lock_table('users')->accessShare();
echo $query->toSql();
// LOCK TABLE users IN ACCESS SHARE MODE

$query = lock_table('users')->rowShare();
echo $query->toSql();
// LOCK TABLE users IN ROW SHARE MODE

$query = lock_table('users')->rowExclusive();
echo $query->toSql();
// LOCK TABLE users IN ROW EXCLUSIVE MODE

$query = lock_table('users')->shareUpdateExclusive();
echo $query->toSql();
// LOCK TABLE users IN SHARE UPDATE EXCLUSIVE MODE

$query = lock_table('users')->share();
echo $query->toSql();
// LOCK TABLE users IN SHARE MODE

$query = lock_table('users')->shareRowExclusive();
echo $query->toSql();
// LOCK TABLE users IN SHARE ROW EXCLUSIVE MODE

$query = lock_table('users')->exclusive();
echo $query->toSql();
// LOCK TABLE users IN EXCLUSIVE MODE

$query = lock_table('users')->accessExclusive();
echo $query->toSql();
// LOCK TABLE users IN ACCESS EXCLUSIVE MODE

// Using enum
$query = lock_table('users')->inMode(LockMode::EXCLUSIVE);
echo $query->toSql();
// LOCK TABLE users IN EXCLUSIVE MODE
```

### LOCK Multiple Tables

```php
<?php

use function Flow\PostgreSql\DSL\lock_table;

$query = lock_table('users', 'orders')
    ->exclusive();

echo $query->toSql();
// LOCK TABLE users, orders IN EXCLUSIVE MODE
```

### LOCK with NOWAIT

```php
<?php

use function Flow\PostgreSql\DSL\lock_table;

$query = lock_table('users')
    ->exclusive()
    ->nowait();

echo $query->toSql();
// LOCK TABLE users IN EXCLUSIVE MODE NOWAIT
```

## Lock Modes

| Mode | Conflicts With |
|------|----------------|
| `ACCESS SHARE` | ACCESS EXCLUSIVE |
| `ROW SHARE` | EXCLUSIVE, ACCESS EXCLUSIVE |
| `ROW EXCLUSIVE` | SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE |
| `SHARE UPDATE EXCLUSIVE` | SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE |
| `SHARE` | ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE ROW EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE |
| `SHARE ROW EXCLUSIVE` | ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE |
| `EXCLUSIVE` | ROW SHARE, ROW EXCLUSIVE, SHARE UPDATE EXCLUSIVE, SHARE, SHARE ROW EXCLUSIVE, EXCLUSIVE, ACCESS EXCLUSIVE |
| `ACCESS EXCLUSIVE` | All modes |

## COMMENT

COMMENT sets or removes a comment on a database object.

### Comment on Table

```php
<?php

use function Flow\PostgreSql\DSL\comment;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;

$query = comment(CommentTarget::TABLE, 'users')
    ->is('User accounts table');

echo $query->toSql();
// COMMENT ON TABLE users IS 'User accounts table'
```

### Comment on Column

```php
<?php

use function Flow\PostgreSql\DSL\comment;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;

$query = comment(CommentTarget::COLUMN, 'users.email')
    ->is('User email address');

echo $query->toSql();
// COMMENT ON COLUMN users.email IS 'User email address'
```

### Comment on Index

```php
<?php

use function Flow\PostgreSql\DSL\comment;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;

$query = comment(CommentTarget::INDEX, 'idx_users_email')
    ->is('Email lookup index');

echo $query->toSql();
// COMMENT ON INDEX idx_users_email IS 'Email lookup index'
```

### Comment on Schema

```php
<?php

use function Flow\PostgreSql\DSL\comment;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;

$query = comment(CommentTarget::SCHEMA, 'public')
    ->is('Default schema');

echo $query->toSql();
// COMMENT ON SCHEMA public IS 'Default schema'
```

### Remove Comment

```php
<?php

use function Flow\PostgreSql\DSL\comment;
use Flow\PostgreSql\QueryBuilder\Utility\CommentTarget;

$query = comment(CommentTarget::TABLE, 'users')
    ->isNull();

echo $query->toSql();
// COMMENT ON TABLE users IS NULL
```

### Comment Targets

The `CommentTarget` enum supports:

| Target | Description |
|--------|-------------|
| `CommentTarget::TABLE` | Table |
| `CommentTarget::COLUMN` | Table column (use dot notation: `table.column`) |
| `CommentTarget::INDEX` | Index |
| `CommentTarget::SCHEMA` | Schema |
| `CommentTarget::SEQUENCE` | Sequence |
| `CommentTarget::VIEW` | View |
| `CommentTarget::MATVIEW` | Materialized view |
| `CommentTarget::FUNCTION` | Function |
| `CommentTarget::PROCEDURE` | Procedure |
| `CommentTarget::TRIGGER` | Trigger |
| `CommentTarget::TYPE` | Type |
| `CommentTarget::EXTENSION` | Extension |
| `CommentTarget::ROLE` | Role |
| `CommentTarget::DATABASE` | Database |

## CLUSTER

CLUSTER physically reorders a table based on an index.

### CLUSTER All Tables

```php
<?php

use function Flow\PostgreSql\DSL\cluster;

$query = cluster();

echo $query->toSql();
// CLUSTER
```

### CLUSTER Single Table

```php
<?php

use function Flow\PostgreSql\DSL\cluster;

$query = cluster()->table('users');

echo $query->toSql();
// CLUSTER users
```

### CLUSTER with Index

```php
<?php

use function Flow\PostgreSql\DSL\cluster;

$query = cluster()->table('users')
    ->using('idx_users_pkey');

echo $query->toSql();
// CLUSTER users USING idx_users_pkey
```

### CLUSTER with Schema

```php
<?php

use function Flow\PostgreSql\DSL\cluster;

$query = cluster()->table('public.users');

echo $query->toSql();
// CLUSTER public.users
```

### CLUSTER with Verbose

```php
<?php

use function Flow\PostgreSql\DSL\cluster;

$query = cluster()
    ->verbose()
    ->table('users');

echo $query->toSql();
// CLUSTER (VERBOSE) users
```

## DISCARD

DISCARD releases session resources.

### DISCARD ALL

```php
<?php

use function Flow\PostgreSql\DSL\discard;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;

$query = discard(DiscardType::ALL);

echo $query->toSql();
// DISCARD ALL
```

### DISCARD PLANS

```php
<?php

use function Flow\PostgreSql\DSL\discard;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;

$query = discard(DiscardType::PLANS);

echo $query->toSql();
// DISCARD PLANS
```

### DISCARD SEQUENCES

```php
<?php

use function Flow\PostgreSql\DSL\discard;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;

$query = discard(DiscardType::SEQUENCES);

echo $query->toSql();
// DISCARD SEQUENCES
```

### DISCARD TEMP

```php
<?php

use function Flow\PostgreSql\DSL\discard;
use Flow\PostgreSql\QueryBuilder\Utility\DiscardType;

$query = discard(DiscardType::TEMP);

echo $query->toSql();
// DISCARD TEMP
```

## Discard Types

| Type | Description |
|------|-------------|
| `DiscardType::ALL` | Release all temporary resources |
| `DiscardType::PLANS` | Release all cached query plans |
| `DiscardType::SEQUENCES` | Reset all sequence-related state |
| `DiscardType::TEMP` | Drop all temporary tables |

## DSL Functions Reference

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
