# Table Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Table Query Builder provides a fluent, type-safe interface for constructing PostgreSQL DDL statements: CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE, and CREATE TABLE AS.

## CREATE TABLE

### Basic Table Creation

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial, sql_type_varchar};

$query = create_table('users')
    ->column(column('id', sql_type_serial())->primaryKey())
    ->column(column('name', sql_type_varchar(100))->notNull());

echo $query->toSQL();
// CREATE TABLE users (id serial PRIMARY KEY, name varchar(100) NOT NULL)
```

### Table with Schema

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial};

$query = create_table('users', 'public')
    ->column(column('id', sql_type_serial())->primaryKey());

echo $query->toSQL();
// CREATE TABLE public.users (id serial PRIMARY KEY)
```

### IF NOT EXISTS

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial};

$query = create_table('users')
    ->ifNotExists()
    ->column(column('id', sql_type_serial())->primaryKey());

echo $query->toSQL();
// CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY)
```

### Column Definitions

Columns support various constraints and options:

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_integer, sql_type_varchar, sql_type_boolean, sql_type_timestamp};

$query = create_table('users')
    ->column(column('id', sql_type_integer())->identity('ALWAYS'))
    ->column(column('email', sql_type_varchar(255))->notNull()->unique())
    ->column(column('active', sql_type_boolean())->default(true))
    ->column(column('created_at', sql_type_timestamp())->defaultRaw('CURRENT_TIMESTAMP'));

echo $query->toSQL();
// CREATE TABLE users (
//   id int GENERATED ALWAYS AS IDENTITY,
//   email varchar(255) NOT NULL UNIQUE,
//   active boolean DEFAULT true,
//   created_at timestamp DEFAULT CURRENT_TIMESTAMP
// )
```

### Column with Foreign Key Reference

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial, sql_type_integer};

$query = create_table('orders')
    ->column(column('id', sql_type_serial())->primaryKey())
    ->column(column('user_id', sql_type_integer())->notNull()->references('users', 'id'));

echo $query->toSQL();
// CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL REFERENCES users(id))
```

### Generated Columns

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_varchar, sql_type_text};

$query = create_table('users')
    ->column(column('first_name', sql_type_varchar(50)))
    ->column(column('last_name', sql_type_varchar(50)))
    ->column(column('full_name', sql_type_text())->generatedAs("first_name || ' ' || last_name"));

echo $query->toSQL();
// CREATE TABLE users (first_name varchar(50), last_name varchar(50), full_name pg_catalog.text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)
```

### Table-Level Constraints

#### Primary Key

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_integer, primary_key};

$query = create_table('order_items')
    ->column(column('order_id', sql_type_integer())->notNull())
    ->column(column('product_id', sql_type_integer())->notNull())
    ->constraint(primary_key('order_id', 'product_id'));

echo $query->toSQL();
// CREATE TABLE order_items (order_id int NOT NULL, product_id int NOT NULL, PRIMARY KEY (order_id, product_id))
```

#### Unique Constraint

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial, sql_type_varchar, unique_constraint};

$query = create_table('users')
    ->column(column('id', sql_type_serial())->primaryKey())
    ->column(column('email', sql_type_varchar(255))->notNull())
    ->constraint(unique_constraint('email'));

echo $query->toSQL();
// CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL, UNIQUE (email))
```

#### Check Constraint

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial, sql_type_integer, check_constraint};

$query = create_table('products')
    ->column(column('id', sql_type_serial())->primaryKey())
    ->column(column('price', sql_type_integer()))
    ->constraint(check_constraint('price > 0')->name('positive_price'));

echo $query->toSQL();
// CREATE TABLE products (id serial PRIMARY KEY, price int, CONSTRAINT positive_price CHECK (price > 0))
```

#### Foreign Key Constraint

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_serial, sql_type_integer, foreign_key, ref_action_cascade, ref_action_restrict};

$query = create_table('orders')
    ->column(column('id', sql_type_serial())->primaryKey())
    ->column(column('user_id', sql_type_integer())->notNull())
    ->constraint(
        foreign_key(['user_id'], 'users', ['id'])
            ->onDelete(ref_action_cascade())
            ->onUpdate(ref_action_restrict())
    );

echo $query->toSQL();
// CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES ONLY users (id) ON UPDATE RESTRICT ON DELETE CASCADE)
```

### Temporary Tables

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_integer};

$query = create_table('temp_results')
    ->temporary()
    ->column(column('id', sql_type_integer()));

echo $query->toSQL();
// CREATE TEMPORARY TABLE temp_results (id int) ON COMMIT DROP
```

### Unlogged Tables

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_integer};

$query = create_table('cache_data')
    ->unlogged()
    ->column(column('id', sql_type_integer()));

echo $query->toSQL();
// CREATE UNLOGGED TABLE cache_data (id int)
```

### Table Inheritance

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_varchar};

$query = create_table('employees')
    ->column(column('department', sql_type_varchar(100)))
    ->inherits('persons');

echo $query->toSQL();
// CREATE TABLE employees (department varchar(100)) INHERITS (persons)
```

### Partitioned Tables

```php
<?php

use function Flow\PgQuery\DSL\{create_table, column, sql_type_integer, sql_type_timestamp};

// Range partitioning
$query = create_table('logs')
    ->column(column('id', sql_type_integer()))
    ->column(column('created_at', sql_type_timestamp()))
    ->partitionByRange('created_at');

echo $query->toSQL();
// CREATE TABLE logs (id int, created_at timestamp) PARTITION BY RANGE (created_at)

// List partitioning
$query = create_table('sales')
    ->column(column('region', sql_type_varchar(50)))
    ->partitionByList('region');

// Hash partitioning
$query = create_table('data')
    ->column(column('id', sql_type_integer()))
    ->partitionByHash('id');
```

## CREATE TABLE AS

### Basic CREATE TABLE AS

```php
<?php

use function Flow\PgQuery\DSL\{create_table_as, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create_table_as('users_backup', $selectQuery);

echo $query->toSQL();
// CREATE TABLE users_backup AS SELECT id, name FROM users
```

### With IF NOT EXISTS

```php
<?php

use function Flow\PgQuery\DSL\{create_table_as, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create_table_as('users_backup', $selectQuery)
    ->ifNotExists();

echo $query->toSQL();
// CREATE TABLE IF NOT EXISTS users_backup AS SELECT id, name FROM users
```

### With Column Names

```php
<?php

use function Flow\PgQuery\DSL\{create_table_as, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create_table_as('users_backup', $selectQuery)
    ->columnNames('user_id', 'user_name');

echo $query->toSQL();
// CREATE TABLE users_backup(user_id, user_name) AS SELECT id, name FROM users
```

### Structure Only (No Data)

```php
<?php

use function Flow\PgQuery\DSL\{create_table_as, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create_table_as('users_backup', $selectQuery)
    ->withNoData();

echo $query->toSQL();
// CREATE TABLE users_backup AS SELECT id, name FROM users WITH NO DATA
```

## ALTER TABLE

### Add Column

```php
<?php

use function Flow\PgQuery\DSL\{alter_table, column, sql_type_varchar};

$query = alter_table('users')
    ->addColumn(column('email', sql_type_varchar(255))->notNull());

echo $query->toSQL();
// ALTER TABLE users ADD COLUMN email varchar(255) NOT NULL
```

### Drop Column

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->dropColumn('temp_column');

echo $query->toSQL();
// ALTER TABLE users DROP temp_column

// With CASCADE
$query = alter_table('users')
    ->dropColumn('temp_column', cascade: true);

echo $query->toSQL();
// ALTER TABLE users DROP temp_column CASCADE
```

### Alter Column Type

```php
<?php

use function Flow\PgQuery\DSL\{alter_table, sql_type_text};

$query = alter_table('users')
    ->alterColumnType('name', sql_type_text());

echo $query->toSQL();
// ALTER TABLE users ALTER COLUMN name TYPE pg_catalog.text
```

### Set/Drop NOT NULL

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

// Set NOT NULL
$query = alter_table('users')
    ->alterColumnSetNotNull('email');

echo $query->toSQL();
// ALTER TABLE users ALTER COLUMN email SET NOT NULL

// Drop NOT NULL
$query = alter_table('users')
    ->alterColumnDropNotNull('email');

echo $query->toSQL();
// ALTER TABLE users ALTER COLUMN email DROP NOT NULL
```

### Set/Drop Default

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

// Set default
$query = alter_table('users')
    ->alterColumnSetDefault('status', "'active'");

echo $query->toSQL();
// ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'

// Drop default
$query = alter_table('users')
    ->alterColumnDropDefault('status');

echo $query->toSQL();
// ALTER TABLE users ALTER COLUMN status DROP DEFAULT
```

### Add Constraint

```php
<?php

use function Flow\PgQuery\DSL\{alter_table, unique_constraint};

$query = alter_table('users')
    ->addConstraint(unique_constraint('email')->name('users_email_unique'));

echo $query->toSQL();
// ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)
```

### Drop Constraint

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->dropConstraint('users_email_unique');

echo $query->toSQL();
// ALTER TABLE users DROP CONSTRAINT users_email_unique
```

### Multiple Operations

```php
<?php

use function Flow\PgQuery\DSL\{alter_table, column, sql_type_varchar};

$query = alter_table('users')
    ->addColumn(column('phone', sql_type_varchar(20)))
    ->dropColumn('fax')
    ->alterColumnSetNotNull('email');

echo $query->toSQL();
// ALTER TABLE users ADD COLUMN phone varchar(20), DROP fax, ALTER COLUMN email SET NOT NULL
```

### IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\{alter_table, column, sql_type_varchar};

$query = alter_table('users')
    ->ifExists()
    ->addColumn(column('email', sql_type_varchar(255)));

echo $query->toSQL();
// ALTER TABLE IF EXISTS users ADD COLUMN email varchar(255)
```

### Rename Column

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->renameColumn('old_name', 'new_name');

echo $query->toSQL();
// ALTER TABLE users RENAME COLUMN old_name TO new_name
```

### Rename Constraint

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->renameConstraint('old_constraint', 'new_constraint');

echo $query->toSQL();
// ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint
```

### Rename Table

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->renameTo('users_archive');

echo $query->toSQL();
// ALTER TABLE users RENAME TO users_archive
```

### Set Schema

```php
<?php

use function Flow\PgQuery\DSL\alter_table;

$query = alter_table('users')
    ->setSchema('archive');

echo $query->toSQL();
// ALTER TABLE users SET SCHEMA archive
```

## DROP TABLE

### Simple Drop

```php
<?php

use function Flow\PgQuery\DSL\drop_table;

$query = drop_table('users');

echo $query->toSQL();
// DROP TABLE users
```

### IF EXISTS

```php
<?php

use function Flow\PgQuery\DSL\drop_table;

$query = drop_table('users')
    ->ifExists();

echo $query->toSQL();
// DROP TABLE IF EXISTS users
```

### CASCADE

```php
<?php

use function Flow\PgQuery\DSL\drop_table;

$query = drop_table('users')
    ->cascade();

echo $query->toSQL();
// DROP TABLE users CASCADE
```

### Multiple Tables

```php
<?php

use function Flow\PgQuery\DSL\drop_table;

$query = drop_table('users', 'orders', 'products');

echo $query->toSQL();
// DROP TABLE users, orders, products
```

## TRUNCATE

### Simple Truncate

```php
<?php

use function Flow\PgQuery\DSL\truncate_table;

$query = truncate_table('users');

echo $query->toSQL();
// TRUNCATE users
```

### Multiple Tables

```php
<?php

use function Flow\PgQuery\DSL\truncate_table;

$query = truncate_table('users', 'orders', 'products');

echo $query->toSQL();
// TRUNCATE users, orders, products
```

### Restart Identity

```php
<?php

use function Flow\PgQuery\DSL\truncate_table;

$query = truncate_table('users')
    ->restartIdentity();

echo $query->toSQL();
// TRUNCATE users RESTART IDENTITY
```

### CASCADE

```php
<?php

use function Flow\PgQuery\DSL\truncate_table;

$query = truncate_table('users')
    ->cascade();

echo $query->toSQL();
// TRUNCATE users CASCADE
```

## Data Types

The following SQL type functions are available:

### Numeric Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `sql_type_integer()` | int |
| `sql_type_bigint()` | bigint |
| `sql_type_smallint()` | smallint |
| `sql_type_serial()` | serial |
| `sql_type_bigserial()` | bigserial |
| `sql_type_numeric($precision, $scale)` | numeric(p,s) |
| `sql_type_decimal($precision, $scale)` | decimal(p,s) |
| `sql_type_real()` | real |
| `sql_type_double()` | double precision |

### String Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `sql_type_text()` | text |
| `sql_type_varchar($length)` | varchar(n) |
| `sql_type_char($length)` | char(n) |

### Date/Time Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `sql_type_date()` | date |
| `sql_type_time($precision)` | time |
| `sql_type_timestamp($precision)` | timestamp |
| `sql_type_timestamptz($precision)` | timestamptz |
| `sql_type_interval()` | interval |

### Other Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `sql_type_boolean()` | boolean |
| `sql_type_uuid()` | uuid |
| `sql_type_json()` | json |
| `sql_type_jsonb()` | jsonb |
| `sql_type_bytea()` | bytea |
| `sql_type_inet()` | inet |
| `sql_type_cidr()` | cidr |
| `sql_type_macaddr()` | macaddr |
| `sql_type_array($elementType)` | type[] |

## Referential Actions

The following referential action functions are available for foreign key constraints:

| Function | Action |
|----------|--------|
| `ref_action_cascade()` | CASCADE |
| `ref_action_restrict()` | RESTRICT |
| `ref_action_no_action()` | NO ACTION |
| `ref_action_set_null()` | SET NULL |
| `ref_action_set_default()` | SET DEFAULT |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
