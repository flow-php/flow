# Table Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Table Query Builder provides a fluent, type-safe interface for constructing PostgreSQL DDL statements: CREATE TABLE, ALTER TABLE, DROP TABLE, TRUNCATE, and CREATE TABLE AS.

## CREATE TABLE

### Basic Table Creation

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial, column_type_varchar};

$query = create()->table('users')
    ->column(column('id', column_type_serial())->primaryKey())
    ->column(column('name', column_type_varchar(100))->notNull());

echo $query->toSql();
// CREATE TABLE users (id serial PRIMARY KEY, name varchar(100) NOT NULL)
```

### Table with Schema

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial};

$query = create()->table('users', 'public')
    ->column(column('id', column_type_serial())->primaryKey());

echo $query->toSql();
// CREATE TABLE public.users (id serial PRIMARY KEY)
```

### IF NOT EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial};

$query = create()->table('users')
    ->ifNotExists()
    ->column(column('id', column_type_serial())->primaryKey());

echo $query->toSql();
// CREATE TABLE IF NOT EXISTS users (id serial PRIMARY KEY)
```

### Column Definitions

Columns support various constraints and options:

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_integer, column_type_varchar, column_type_boolean, column_type_timestamp};

$query = create()->table('users')
    ->column(column('id', column_type_integer())->identity('ALWAYS'))
    ->column(column('email', column_type_varchar(255))->notNull()->unique())
    ->column(column('active', column_type_boolean())->default(true))
    ->column(column('created_at', column_type_timestamp())->defaultRaw('CURRENT_TIMESTAMP'));

echo $query->toSql();
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

use function Flow\PostgreSql\DSL\{create, column, column_type_serial, column_type_integer};

$query = create()->table('orders')
    ->column(column('id', column_type_serial())->primaryKey())
    ->column(column('user_id', column_type_integer())->notNull()->references('users', 'id'));

echo $query->toSql();
// CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL REFERENCES users(id))
```

### Generated Columns

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_varchar, column_type_text};

$query = create()->table('users')
    ->column(column('first_name', column_type_varchar(50)))
    ->column(column('last_name', column_type_varchar(50)))
    ->column(column('full_name', column_type_text())->generatedAs("first_name || ' ' || last_name"));

echo $query->toSql();
// CREATE TABLE users (first_name varchar(50), last_name varchar(50), full_name pg_catalog.text GENERATED ALWAYS AS (first_name || ' ' || last_name) STORED)
```

### Table-Level Constraints

#### Primary Key

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_integer, primary_key};

$query = create()->table('order_items')
    ->column(column('order_id', column_type_integer())->notNull())
    ->column(column('product_id', column_type_integer())->notNull())
    ->constraint(primary_key('order_id', 'product_id'));

echo $query->toSql();
// CREATE TABLE order_items (order_id int NOT NULL, product_id int NOT NULL, PRIMARY KEY (order_id, product_id))
```

#### Unique Constraint

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial, column_type_varchar, unique_constraint};

$query = create()->table('users')
    ->column(column('id', column_type_serial())->primaryKey())
    ->column(column('email', column_type_varchar(255))->notNull())
    ->constraint(unique_constraint('email'));

echo $query->toSql();
// CREATE TABLE users (id serial PRIMARY KEY, email varchar(255) NOT NULL, UNIQUE (email))
```

#### Check Constraint

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial, column_type_integer, check_constraint};

$query = create()->table('products')
    ->column(column('id', column_type_serial())->primaryKey())
    ->column(column('price', column_type_integer()))
    ->constraint(check_constraint('price > 0')->name('positive_price'));

echo $query->toSql();
// CREATE TABLE products (id serial PRIMARY KEY, price int, CONSTRAINT positive_price CHECK (price > 0))
```

#### Foreign Key Constraint

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_serial, column_type_integer, foreign_key, ref_action_cascade, ref_action_restrict};

$query = create()->table('orders')
    ->column(column('id', column_type_serial())->primaryKey())
    ->column(column('user_id', column_type_integer())->notNull())
    ->constraint(
        foreign_key(['user_id'], 'users', ['id'])
            ->onDelete(ref_action_cascade())
            ->onUpdate(ref_action_restrict())
    );

echo $query->toSql();
// CREATE TABLE orders (id serial PRIMARY KEY, user_id int NOT NULL, FOREIGN KEY (user_id) REFERENCES ONLY users (id) ON UPDATE RESTRICT ON DELETE CASCADE)
```

### Temporary Tables

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_integer};

$query = create()->table('temp_results')
    ->column(column('id', column_type_integer()))
    ->temporary();

echo $query->toSql();
// CREATE TEMPORARY TABLE temp_results (id int)
```

### Unlogged Tables

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_integer};

$query = create()->table('cache_data')
    ->unlogged()
    ->column(column('id', column_type_integer()));

echo $query->toSql();
// CREATE UNLOGGED TABLE cache_data (id int)
```

### Table Inheritance

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_varchar};

$query = create()->table('employees')
    ->column(column('department', column_type_varchar(100)))
    ->inherits('persons');

echo $query->toSql();
// CREATE TABLE employees (department varchar(100)) INHERITS (persons)
```

### Partitioned Tables

```php
<?php

use function Flow\PostgreSql\DSL\{create, column, column_type_integer, column_type_timestamp};

// Range partitioning
$query = create()->table('logs')
    ->column(column('id', column_type_integer()))
    ->column(column('created_at', column_type_timestamp()))
    ->partitionByRange('created_at');

echo $query->toSql();
// CREATE TABLE logs (id int, created_at timestamp) PARTITION BY RANGE (created_at)

// List partitioning
$query = create()->table('sales')
    ->column(column('region', column_type_varchar(50)))
    ->partitionByList('region');

// Hash partitioning
$query = create()->table('data')
    ->column(column('id', column_type_integer()))
    ->partitionByHash('id');
```

## CREATE TABLE AS

### Basic CREATE TABLE AS

```php
<?php

use function Flow\PostgreSql\DSL\{create, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create()->tableAs('users_backup', $selectQuery);

echo $query->toSql();
// CREATE TABLE users_backup AS SELECT id, name FROM users
```

### With IF NOT EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\{create, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create()->tableAs('users_backup', $selectQuery)
    ->ifNotExists();

echo $query->toSql();
// CREATE TABLE IF NOT EXISTS users_backup AS SELECT id, name FROM users
```

### With Column Names

```php
<?php

use function Flow\PostgreSql\DSL\{create, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create()->tableAs('users_backup', $selectQuery)
    ->columnNames('user_id', 'user_name');

echo $query->toSql();
// CREATE TABLE users_backup(user_id, user_name) AS SELECT id, name FROM users
```

### Structure Only (No Data)

```php
<?php

use function Flow\PostgreSql\DSL\{create, select, col, table};

$selectQuery = select()
    ->select(col('id'), col('name'))
    ->from(table('users'));

$query = create()->tableAs('users_backup', $selectQuery)
    ->withNoData();

echo $query->toSql();
// CREATE TABLE users_backup AS SELECT id, name FROM users WITH NO DATA
```

## ALTER TABLE

### Add Column

```php
<?php

use function Flow\PostgreSql\DSL\{alter, column, column_type_varchar};

$query = alter()->table('users')
    ->addColumn(column('email', column_type_varchar(255))->notNull());

echo $query->toSql();
// ALTER TABLE users ADD COLUMN email varchar(255) NOT NULL
```

### Drop Column

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->dropColumn('temp_column');

echo $query->toSql();
// ALTER TABLE users DROP temp_column

// With CASCADE
$query = alter()->table('users')
    ->dropColumn('temp_column', cascade: true);

echo $query->toSql();
// ALTER TABLE users DROP temp_column CASCADE
```

### Alter Column Type

```php
<?php

use function Flow\PostgreSql\DSL\{alter, column_type_text};

$query = alter()->table('users')
    ->alterColumnType('name', column_type_text());

echo $query->toSql();
// ALTER TABLE users ALTER COLUMN name TYPE pg_catalog.text
```

### Set/Drop NOT NULL

```php
<?php

use function Flow\PostgreSql\DSL\alter;

// Set NOT NULL
$query = alter()->table('users')
    ->alterColumnSetNotNull('email');

echo $query->toSql();
// ALTER TABLE users ALTER COLUMN email SET NOT NULL

// Drop NOT NULL
$query = alter()->table('users')
    ->alterColumnDropNotNull('email');

echo $query->toSql();
// ALTER TABLE users ALTER COLUMN email DROP NOT NULL
```

### Set/Drop Default

```php
<?php

use function Flow\PostgreSql\DSL\alter;

// Set default
$query = alter()->table('users')
    ->alterColumnSetDefault('status', "'active'");

echo $query->toSql();
// ALTER TABLE users ALTER COLUMN status SET DEFAULT 'active'

// Drop default
$query = alter()->table('users')
    ->alterColumnDropDefault('status');

echo $query->toSql();
// ALTER TABLE users ALTER COLUMN status DROP DEFAULT
```

### Add Constraint

```php
<?php

use function Flow\PostgreSql\DSL\{alter, unique_constraint};

$query = alter()->table('users')
    ->addConstraint(unique_constraint('email')->name('users_email_unique'));

echo $query->toSql();
// ALTER TABLE users ADD CONSTRAINT users_email_unique UNIQUE (email)
```

### Drop Constraint

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->dropConstraint('users_email_unique');

echo $query->toSql();
// ALTER TABLE users DROP CONSTRAINT users_email_unique
```

### Multiple Operations

```php
<?php

use function Flow\PostgreSql\DSL\{alter, column, column_type_varchar};

$query = alter()->table('users')
    ->addColumn(column('phone', column_type_varchar(20)))
    ->dropColumn('fax')
    ->alterColumnSetNotNull('email');

echo $query->toSql();
// ALTER TABLE users ADD COLUMN phone varchar(20), DROP fax, ALTER COLUMN email SET NOT NULL
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\{alter, column, column_type_varchar};

$query = alter()->table('users')
    ->ifExists()
    ->addColumn(column('email', column_type_varchar(255)));

echo $query->toSql();
// ALTER TABLE IF EXISTS users ADD COLUMN email varchar(255)
```

### Rename Column

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->renameColumn('old_name', 'new_name');

echo $query->toSql();
// ALTER TABLE users RENAME COLUMN old_name TO new_name
```

### Rename Constraint

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->renameConstraint('old_constraint', 'new_constraint');

echo $query->toSql();
// ALTER TABLE users RENAME CONSTRAINT old_constraint TO new_constraint
```

### Rename Table

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->renameTo('users_archive');

echo $query->toSql();
// ALTER TABLE users RENAME TO users_archive
```

### Set Schema

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->table('users')
    ->setSchema('archive');

echo $query->toSql();
// ALTER TABLE users SET SCHEMA archive
```

## DROP TABLE

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->table('users');

echo $query->toSql();
// DROP TABLE users
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->table('users')
    ->ifExists();

echo $query->toSql();
// DROP TABLE IF EXISTS users
```

### CASCADE

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->table('users')
    ->cascade();

echo $query->toSql();
// DROP TABLE users CASCADE
```

### Multiple Tables

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->table('users', 'orders', 'products');

echo $query->toSql();
// DROP TABLE users, orders, products
```

## TRUNCATE

### Simple Truncate

```php
<?php

use function Flow\PostgreSql\DSL\truncate_table;

$query = truncate_table('users');

echo $query->toSql();
// TRUNCATE users
```

### Multiple Tables

```php
<?php

use function Flow\PostgreSql\DSL\truncate_table;

$query = truncate_table('users', 'orders', 'products');

echo $query->toSql();
// TRUNCATE users, orders, products
```

### Restart Identity

```php
<?php

use function Flow\PostgreSql\DSL\truncate_table;

$query = truncate_table('users')
    ->restartIdentity();

echo $query->toSql();
// TRUNCATE users RESTART IDENTITY
```

### CASCADE

```php
<?php

use function Flow\PostgreSql\DSL\truncate_table;

$query = truncate_table('users')
    ->cascade();

echo $query->toSql();
// TRUNCATE users CASCADE
```

## Data Types

The following SQL type functions are available:

### Numeric Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `column_type_integer()` | int |
| `column_type_bigint()` | bigint |
| `column_type_smallint()` | smallint |
| `column_type_serial()` | serial |
| `column_type_bigserial()` | bigserial |
| `column_type_numeric($precision, $scale)` | numeric(p,s) |
| `column_type_decimal($precision, $scale)` | decimal(p,s) |
| `column_type_real()` | real |
| `column_type_double()` | double precision |

### String Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `column_type_text()` | text |
| `column_type_varchar($length)` | varchar(n) |
| `column_type_char($length)` | char(n) |

### Date/Time Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `column_type_date()` | date |
| `column_type_time($precision)` | time |
| `column_type_timestamp($precision)` | timestamp |
| `column_type_timestamptz($precision)` | timestamptz |
| `column_type_interval()` | interval |

### Other Types

| Function | PostgreSQL Type |
|----------|-----------------|
| `column_type_boolean()` | boolean |
| `column_type_uuid()` | uuid |
| `column_type_json()` | json |
| `column_type_jsonb()` | jsonb |
| `column_type_bytea()` | bytea |
| `column_type_inet()` | inet |
| `column_type_cidr()` | cidr |
| `column_type_macaddr()` | macaddr |
| `column_type_array($elementType)` | type[] |

## Referential Actions

The following referential action functions are available for foreign key constraints:

| Function | Action |
|----------|--------|
| `ref_action_cascade()` | CASCADE |
| `ref_action_restrict()` | RESTRICT |
| `ref_action_no_action()` | NO ACTION |
| `ref_action_set_null()` | SET NULL |
| `ref_action_set_default()` | SET DEFAULT |

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
