# Index Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Index Query Builder provides a fluent, type-safe interface for constructing PostgreSQL index management statements: CREATE INDEX, DROP INDEX, REINDEX, and ALTER INDEX.

## CREATE INDEX

### Basic Index Creation

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->on('users')
    ->columns('email');

echo $query->toSQL();
// CREATE INDEX idx_users_email ON users (email)
```

### Unique Index

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->unique()
    ->on('users')
    ->columns('email');

echo $query->toSQL();
// CREATE UNIQUE INDEX idx_users_email ON users (email)
```

### IF NOT EXISTS

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->ifNotExists()
    ->on('users')
    ->columns('email');

echo $query->toSQL();
// CREATE INDEX IF NOT EXISTS idx_users_email ON users (email)
```

### CONCURRENTLY

Create an index without locking out concurrent inserts, updates, or deletes:

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->concurrently()
    ->on('users')
    ->columns('email');

echo $query->toSQL();
// CREATE INDEX CONCURRENTLY idx_users_email ON users (email)
```

### Combined Modifiers

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->unique()
    ->concurrently()
    ->ifNotExists()
    ->on('users')
    ->columns('email');

echo $query->toSQL();
// CREATE UNIQUE INDEX CONCURRENTLY IF NOT EXISTS idx_users_email ON users (email)
```

### Index with Schema

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->on('users', 'public')
    ->columns('email');

echo $query->toSQL();
// CREATE INDEX idx_users_email ON public.users (email)
```

### Index Access Methods

PostgreSQL supports different index access methods. Use the `using()` method with an `IndexMethod`:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, index_method_btree, index_method_hash, index_method_gin, index_method_gist, index_method_spgist, index_method_brin};

// B-tree (default, good for equality and range queries)
$query = create_index('idx_users_email')
    ->on('users')
    ->using(index_method_btree())
    ->columns('email');

// Hash (good for equality comparisons only)
$query = create_index('idx_users_email')
    ->on('users')
    ->using(index_method_hash())
    ->columns('email');

// GIN (good for array values, full-text search, jsonb)
$query = create_index('idx_documents_content')
    ->on('documents')
    ->using(index_method_gin())
    ->columns('content');

// GiST (good for geometric data, full-text search)
$query = create_index('idx_locations_point')
    ->on('locations')
    ->using(index_method_gist())
    ->columns('point');

// SP-GiST (good for non-balanced data like phone numbers)
$query = create_index('idx_users_phone')
    ->on('users')
    ->using(index_method_spgist())
    ->columns('phone');

// BRIN (good for large tables with naturally ordered data)
$query = create_index('idx_events_timestamp')
    ->on('events')
    ->using(index_method_brin())
    ->columns('created_at');
```

### Multiple Columns

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_name_email')
    ->on('users')
    ->columns('name', 'email');

echo $query->toSQL();
// CREATE INDEX idx_users_name_email ON users (name, email)
```

### Column Sort Order

Use `index_col()` to customize column ordering:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, index_col};

// Descending order
$query = create_index('idx_users_created_at')
    ->on('users')
    ->columns(index_col('created_at')->desc());

echo $query->toSQL();
// CREATE INDEX idx_users_created_at ON users (created_at DESC)

// With NULLS ordering
$query = create_index('idx_users_created_at')
    ->on('users')
    ->columns(index_col('created_at')->desc()->nullsFirst());

echo $query->toSQL();
// CREATE INDEX idx_users_created_at ON users (created_at DESC NULLS FIRST)

$query = create_index('idx_users_created_at')
    ->on('users')
    ->columns(index_col('created_at')->nullsLast());

echo $query->toSQL();
// CREATE INDEX idx_users_created_at ON users (created_at NULLS LAST)
```

### Operator Class

Specify an operator class for index columns:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, index_col};

$query = create_index('idx_users_name_pattern')
    ->on('users')
    ->columns(index_col('name')->opclass('text_pattern_ops'));

echo $query->toSQL();
// CREATE INDEX idx_users_name_pattern ON users (name text_pattern_ops)
```

### Collation

Specify a collation for index columns:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, index_col};

$query = create_index('idx_users_name')
    ->on('users')
    ->columns(index_col('name')->collate('en_US'));
```

### Expression Index

Create an index on an expression:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, index_expr};
use Flow\PgQuery\QueryBuilder\Expression\RawExpression;

$query = create_index('idx_users_lower_email')
    ->on('users')
    ->columns(index_expr(new RawExpression('lower(email)')));

echo $query->toSQL();
// CREATE INDEX idx_users_lower_email ON users ((lower(email)))
```

### Covering Index (INCLUDE)

Include additional columns in the index for index-only scans:

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->on('users')
    ->columns('email')
    ->include('name', 'created_at');

echo $query->toSQL();
// CREATE INDEX idx_users_email ON users (email) INCLUDE (name, created_at)
```

### Partial Index (WHERE)

Create an index on a subset of rows:

```php
<?php

use function Flow\PgQuery\DSL\{create_index, col, eq, literal_bool};

$query = create_index('idx_users_active_email')
    ->on('users')
    ->columns('email')
    ->where(eq(col('active'), literal_bool(true)));

echo $query->toSQL();
// CREATE INDEX idx_users_active_email ON users (email) WHERE active = true
```

### Tablespace

Specify a tablespace for the index:

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->on('users')
    ->columns('email')
    ->tablespace('fast_storage');

echo $query->toSQL();
// CREATE INDEX idx_users_email ON users (email) TABLESPACE fast_storage
```

### NULLS NOT DISTINCT

For unique indexes, treat NULL values as not distinct:

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->unique()
    ->on('users')
    ->columns('email')
    ->nullsNotDistinct();

echo $query->toSQL();
// CREATE UNIQUE INDEX idx_users_email ON users (email) NULLS NOT DISTINCT
```

### ON ONLY (Partitioned Tables)

Create an index on only the specified table, not including child partitions:

```php
<?php

use function Flow\PgQuery\DSL\create_index;

$query = create_index('idx_users_email')
    ->onOnly('users')
    ->columns('email');

echo $query->toSQL();
// CREATE INDEX idx_users_email ON ONLY users (email)
```

## DROP INDEX

### Simple Drop

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email');

echo $query->toSQL();
// DROP INDEX idx_users_email
```

### IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email')
    ->ifExists();

echo $query->toSQL();
// DROP INDEX IF EXISTS idx_users_email
```

### CONCURRENTLY

Drop an index without locking out concurrent selects, inserts, updates, or deletes:

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email')
    ->concurrently();

echo $query->toSQL();
// DROP INDEX CONCURRENTLY idx_users_email
```

### CASCADE

Drop objects that depend on the index:

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email')
    ->cascade();

echo $query->toSQL();
// DROP INDEX idx_users_email CASCADE
```

### Combined Options

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email')
    ->ifExists()
    ->cascade();

echo $query->toSQL();
// DROP INDEX IF EXISTS idx_users_email CASCADE
```

### Multiple Indexes

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('idx_users_email', 'idx_users_name', 'idx_orders_date');

echo $query->toSQL();
// DROP INDEX idx_users_email, idx_users_name, idx_orders_date
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\drop_index;

$query = drop_index('public.idx_users_email');

echo $query->toSQL();
// DROP INDEX public.idx_users_email
```

## REINDEX

### Reindex an Index

```php
<?php

use function Flow\PgQuery\DSL\reindex_index;

$query = reindex_index('idx_users_email');

echo $query->toSQL();
// REINDEX INDEX idx_users_email
```

### Reindex a Table

Rebuild all indexes on a table:

```php
<?php

use function Flow\PgQuery\DSL\reindex_table;

$query = reindex_table('users');

echo $query->toSQL();
// REINDEX TABLE users
```

### Reindex a Schema

Rebuild all indexes in a schema:

```php
<?php

use function Flow\PgQuery\DSL\reindex_schema;

$query = reindex_schema('public');

echo $query->toSQL();
// REINDEX SCHEMA public
```

### Reindex a Database

Rebuild all indexes in the current database:

```php
<?php

use function Flow\PgQuery\DSL\reindex_database;

$query = reindex_database('mydb');

echo $query->toSQL();
// REINDEX DATABASE mydb
```

### CONCURRENTLY

Rebuild an index without locking out writes:

```php
<?php

use function Flow\PgQuery\DSL\reindex_index;

$query = reindex_index('idx_users_email')
    ->concurrently();

echo $query->toSQL();
// REINDEX (CONCURRENTLY) INDEX idx_users_email
```

### VERBOSE

Print progress reports:

```php
<?php

use function Flow\PgQuery\DSL\reindex_table;

$query = reindex_table('users')
    ->verbose();

echo $query->toSQL();
// REINDEX (VERBOSE) TABLE users
```

### TABLESPACE

Rebuild the index in a different tablespace:

```php
<?php

use function Flow\PgQuery\DSL\reindex_index;

$query = reindex_index('idx_users_email')
    ->tablespace('fast_storage');

echo $query->toSQL();
// REINDEX (TABLESPACE fast_storage) INDEX idx_users_email
```

### With Schema

```php
<?php

use function Flow\PgQuery\DSL\reindex_table;

$query = reindex_table('public.users');

echo $query->toSQL();
// REINDEX TABLE public.users
```

## ALTER INDEX

### Rename Index

```php
<?php

use function Flow\PgQuery\DSL\alter_index;

$query = alter_index('idx_old')
    ->renameTo('idx_new');

echo $query->toSQL();
// ALTER INDEX idx_old RENAME TO idx_new
```

### Rename with IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\alter_index;

$query = alter_index('idx_old')
    ->ifExists()
    ->renameTo('idx_new');

echo $query->toSQL();
// ALTER INDEX IF EXISTS idx_old RENAME TO idx_new
```

### Rename with Schema

```php
<?php

use function Flow\PgQuery\DSL\alter_index;

$query = alter_index('idx_old', 'public')
    ->renameTo('idx_new');

echo $query->toSQL();
// ALTER INDEX public.idx_old RENAME TO idx_new
```

### Set Tablespace

Move an index to a different tablespace:

```php
<?php

use function Flow\PgQuery\DSL\alter_index;

$query = alter_index('idx_users_email')
    ->setTablespace('fast_storage');

echo $query->toSQL();
// ALTER INDEX idx_users_email SET TABLESPACE fast_storage
```

### Set Tablespace with IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\alter_index;

$query = alter_index('idx_users_email')
    ->ifExists()
    ->setTablespace('fast_storage');

echo $query->toSQL();
// ALTER INDEX IF EXISTS idx_users_email SET TABLESPACE fast_storage
```

## Index Methods Reference

| Function | PostgreSQL Method | Best For |
|----------|-------------------|----------|
| `index_method_btree()` | btree | Equality and range queries (default) |
| `index_method_hash()` | hash | Equality comparisons only |
| `index_method_gist()` | gist | Geometric data, full-text search |
| `index_method_spgist()` | spgist | Non-balanced data (phone numbers, IP ranges) |
| `index_method_gin()` | gin | Arrays, full-text search, JSONB |
| `index_method_brin()` | brin | Large tables with naturally ordered data |

## IndexColumn Options Reference

The `index_col()` function creates an `IndexColumn` with the following chainable methods:

| Method | Description |
|--------|-------------|
| `asc()` | Sort ascending (default) |
| `desc()` | Sort descending |
| `nullsFirst()` | NULLs sort before non-null values |
| `nullsLast()` | NULLs sort after non-null values |
| `opclass(string)` | Specify operator class |
| `collate(string)` | Specify collation |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
