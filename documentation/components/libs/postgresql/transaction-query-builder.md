# Transaction Query Builder

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The Transaction Query Builder provides a fluent, type-safe interface for constructing PostgreSQL transaction control statements. It supports BEGIN, COMMIT, ROLLBACK, SAVEPOINT, SET TRANSACTION, and two-phase commit operations.

## BEGIN

### Basic BEGIN

```php
<?php

use function Flow\PostgreSql\DSL\begin;

$query = begin();

echo $query->toSQL();
// BEGIN
```

### BEGIN with Isolation Level

```php
<?php

use function Flow\PostgreSql\DSL\begin;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

$query = begin()
    ->isolationLevel(IsolationLevel::SERIALIZABLE);

echo $query->toSQL();
// BEGIN ISOLATION LEVEL SERIALIZABLE
```

### BEGIN with Read Mode

```php
<?php

use function Flow\PostgreSql\DSL\begin;

// Read-only transaction
$query = begin()
    ->readOnly();

echo $query->toSQL();
// BEGIN READ ONLY

// Read-write transaction (explicit)
$query = begin()
    ->readWrite();

echo $query->toSQL();
// BEGIN READ WRITE
```

### BEGIN with Deferrable

Deferrable transactions are useful for long-running read-only queries that need SERIALIZABLE isolation:

```php
<?php

use function Flow\PostgreSql\DSL\begin;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

$query = begin()
    ->isolationLevel(IsolationLevel::SERIALIZABLE)
    ->readOnly()
    ->deferrable();

echo $query->toSQL();
// BEGIN ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE
```

## COMMIT

### Basic COMMIT

```php
<?php

use function Flow\PostgreSql\DSL\commit;

$query = commit();

echo $query->toSQL();
// COMMIT
```

### COMMIT AND CHAIN

Start a new transaction with the same characteristics immediately after committing:

```php
<?php

use function Flow\PostgreSql\DSL\commit;

$query = commit()
    ->andChain();

echo $query->toSQL();
// COMMIT AND CHAIN
```

## ROLLBACK

### Basic ROLLBACK

```php
<?php

use function Flow\PostgreSql\DSL\rollback;

$query = rollback();

echo $query->toSQL();
// ROLLBACK
```

### ROLLBACK TO SAVEPOINT

Roll back to a specific savepoint:

```php
<?php

use function Flow\PostgreSql\DSL\rollback;

$query = rollback()
    ->toSavepoint('my_savepoint');

echo $query->toSQL();
// ROLLBACK TO SAVEPOINT my_savepoint
```

### ROLLBACK AND CHAIN

```php
<?php

use function Flow\PostgreSql\DSL\rollback;

$query = rollback()
    ->andChain();

echo $query->toSQL();
// ROLLBACK AND CHAIN
```

## SAVEPOINT

### Create a Savepoint

```php
<?php

use function Flow\PostgreSql\DSL\savepoint;

$query = savepoint('my_savepoint');

echo $query->toSQL();
// SAVEPOINT my_savepoint
```

### Release a Savepoint

```php
<?php

use function Flow\PostgreSql\DSL\release_savepoint;

$query = release_savepoint('my_savepoint');

echo $query->toSQL();
// RELEASE my_savepoint
```

## SET TRANSACTION

### Set Transaction Isolation Level

```php
<?php

use function Flow\PostgreSql\DSL\set_transaction;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

$query = set_transaction()
    ->isolationLevel(IsolationLevel::SERIALIZABLE);

echo $query->toSQL();
// SET TRANSACTION ISOLATION LEVEL SERIALIZABLE
```

### Set Transaction Read Mode

```php
<?php

use function Flow\PostgreSql\DSL\set_transaction;

$query = set_transaction()
    ->readOnly();

echo $query->toSQL();
// SET TRANSACTION READ ONLY
```

### Set Multiple Transaction Options

```php
<?php

use function Flow\PostgreSql\DSL\set_transaction;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

$query = set_transaction()
    ->isolationLevel(IsolationLevel::SERIALIZABLE)
    ->readOnly()
    ->deferrable();

echo $query->toSQL();
// SET TRANSACTION ISOLATION LEVEL SERIALIZABLE, READ ONLY, DEFERRABLE
```

### Set Session Transaction Defaults

Set default transaction characteristics for the session:

```php
<?php

use function Flow\PostgreSql\DSL\set_session_transaction;
use Flow\PostgreSql\QueryBuilder\Transaction\IsolationLevel;

$query = set_session_transaction()
    ->isolationLevel(IsolationLevel::SERIALIZABLE);

echo $query->toSQL();
// SET SESSION CHARACTERISTICS AS TRANSACTION ISOLATION LEVEL SERIALIZABLE
```

### Transaction Snapshot

Import a snapshot from another session (for parallel queries):

```php
<?php

use function Flow\PostgreSql\DSL\transaction_snapshot;

$query = transaction_snapshot('00000003-0000001A-1');

echo $query->toSQL();
// SET TRANSACTION SNAPSHOT '00000003-0000001A-1'
```

## Two-Phase Commit (Prepared Transactions)

Two-phase commit is useful for distributed transactions across multiple databases.

### Prepare Transaction

```php
<?php

use function Flow\PostgreSql\DSL\prepare_transaction;

$query = prepare_transaction('my_transaction');

echo $query->toSQL();
// PREPARE TRANSACTION 'my_transaction'
```

### Commit Prepared

```php
<?php

use function Flow\PostgreSql\DSL\commit_prepared;

$query = commit_prepared('my_transaction');

echo $query->toSQL();
// COMMIT PREPARED 'my_transaction'
```

### Rollback Prepared

```php
<?php

use function Flow\PostgreSql\DSL\rollback_prepared;

$query = rollback_prepared('my_transaction');

echo $query->toSQL();
// ROLLBACK PREPARED 'my_transaction'
```

## Isolation Levels

The `IsolationLevel` enum provides four standard SQL isolation levels:

| Level | Description |
|-------|-------------|
| `IsolationLevel::READ_UNCOMMITTED` | Allows dirty reads (treated as READ COMMITTED in PostgreSQL) |
| `IsolationLevel::READ_COMMITTED` | Default level. Only sees committed data |
| `IsolationLevel::REPEATABLE_READ` | Sees a snapshot from transaction start |
| `IsolationLevel::SERIALIZABLE` | Strictest level. Transactions appear to execute serially |

## DSL Functions Reference

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
