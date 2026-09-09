# View Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The View Query Builder provides a fluent, type-safe interface for constructing PostgreSQL view management statements:
CREATE VIEW, CREATE MATERIALIZED VIEW, ALTER VIEW, ALTER MATERIALIZED VIEW, DROP VIEW, DROP MATERIALIZED VIEW, and REFRESH MATERIALIZED VIEW.

## CREATE VIEW

### Basic View Creation

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->view('active_users')
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE VIEW active_users AS SELECT * FROM users
```

### With Schema

Schema can be specified as a separate parameter or as part of the name:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

// Using schema.name format
$query = create()->view('public.active_users')
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE VIEW public.active_users AS SELECT * FROM users

// Using separate schema parameter
$query = create()->view('active_users', 'public')
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE VIEW public.active_users AS SELECT * FROM users
```

### OR REPLACE

Create or replace an existing view:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->view('active_users')
    ->orReplace()
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE OR REPLACE VIEW active_users AS SELECT * FROM users
```

### TEMPORARY

Create a temporary view that is automatically dropped at the end of the session:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->view('temp_users')
    ->temporary()
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE TEMPORARY VIEW temp_users AS SELECT * FROM users
```

### Column Aliases

Define column aliases for the view:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->view('user_info')
    ->columns('user_id', 'user_name', 'email_address')
    ->as(select('id', 'name', 'email')->from('users'));

echo $query->toSql();
// CREATE VIEW user_info (user_id, user_name, email_address) AS SELECT id, name, email FROM users
```

### WITH CHECK OPTION

Add check options for updatable views:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select, eq, col, literal};

// WITH CHECK OPTION (defaults to CASCADED)
$query = create()->view('active_users')
    ->as(select()->from('users')->where(eq(col('active'), literal(true))))
    ->withCheckOption();

echo $query->toSql();
// CREATE VIEW active_users AS SELECT * FROM users WHERE active = true WITH CHECK OPTION

// WITH CASCADED CHECK OPTION
$query = create()->view('active_users')
    ->as(select()->from('users'))
    ->withCascadedCheckOption();

echo $query->toSql();
// CREATE VIEW active_users AS SELECT * FROM users WITH CASCADED CHECK OPTION

// WITH LOCAL CHECK OPTION
$query = create()->view('active_users')
    ->as(select()->from('users'))
    ->withLocalCheckOption();

echo $query->toSql();
// CREATE VIEW active_users AS SELECT * FROM users WITH LOCAL CHECK OPTION
```

## CREATE MATERIALIZED VIEW

### Basic Materialized View Creation

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('user_stats')
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users
```

### IF NOT EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('user_stats')
    ->ifNotExists()
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE MATERIALIZED VIEW IF NOT EXISTS user_stats AS SELECT * FROM users
```

### Column Aliases

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('user_stats')
    ->columns('user_id', 'order_count')
    ->as(select('id', 'count(*)')->from('users'));

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats (user_id, order_count) AS SELECT id, count(*) FROM users
```

### USING (Access Method)

Specify a storage access method:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('user_stats')
    ->using('heap')
    ->as(select()->from('users'));

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats USING heap AS SELECT * FROM users
```

### TABLESPACE

Specify the tablespace for the materialized view:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('user_stats')
    ->as(select()->from('users'))
    ->tablespace('fast_storage');

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats TABLESPACE fast_storage AS SELECT * FROM users
```

### WITH DATA / WITH NO DATA

Control whether to populate the materialized view immediately:

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

// Populate immediately (default behavior)
$query = create()->materializedView('user_stats')
    ->as(select()->from('users'))
    ->withData();

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users WITH DATA

// Create empty, populate later with REFRESH
$query = create()->materializedView('user_stats')
    ->as(select()->from('users'))
    ->withNoData();

echo $query->toSql();
// CREATE MATERIALIZED VIEW user_stats AS SELECT * FROM users WITH NO DATA
```

### Complete Example

```php
<?php

use function Flow\PostgreSql\DSL\{create, select};

$query = create()->materializedView('analytics.user_stats')
    ->ifNotExists()
    ->using('heap')
    ->columns('user_id', 'order_count', 'total_spent')
    ->as(select('id', 'count(*)', 'sum(total)')->from('users'))
    ->tablespace('analytics_ts')
    ->withNoData();

echo $query->toSql();
// CREATE MATERIALIZED VIEW IF NOT EXISTS analytics.user_stats (user_id, order_count, total_spent)
//   USING heap TABLESPACE analytics_ts AS SELECT id, count(*), sum(total) FROM users WITH NO DATA
```

## ALTER VIEW

### RENAME TO

Rename a view:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->view('old_view')
    ->renameTo('new_view');

echo $query->toSql();
// ALTER VIEW old_view RENAME TO new_view
```

### IF EXISTS

Only alter the view if it exists:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->view('old_view')
    ->ifExists()
    ->renameTo('new_view');

echo $query->toSql();
// ALTER VIEW IF EXISTS old_view RENAME TO new_view
```

### SET SCHEMA

Move a view to a different schema:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->view('my_view')
    ->setSchema('archive');

echo $query->toSql();
// ALTER VIEW my_view SET SCHEMA archive
```

### OWNER TO

Change the owner of a view:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->view('my_view')
    ->ownerTo('new_owner');

echo $query->toSql();
// ALTER VIEW my_view OWNER TO new_owner
```

## ALTER MATERIALIZED VIEW

### RENAME TO

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->materializedView('old_matview')
    ->renameTo('new_matview');

echo $query->toSql();
// ALTER MATERIALIZED VIEW old_matview RENAME TO new_matview
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->materializedView('old_matview')
    ->ifExists()
    ->renameTo('new_matview');

echo $query->toSql();
// ALTER MATERIALIZED VIEW IF EXISTS old_matview RENAME TO new_matview
```

### SET SCHEMA

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->materializedView('my_matview')
    ->setSchema('archive');

echo $query->toSql();
// ALTER MATERIALIZED VIEW my_matview SET SCHEMA archive
```

### OWNER TO

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->materializedView('my_matview')
    ->ownerTo('new_owner');

echo $query->toSql();
// ALTER MATERIALIZED VIEW my_matview OWNER TO new_owner
```

### SET TABLESPACE

Move a materialized view to a different tablespace:

```php
<?php

use function Flow\PostgreSql\DSL\alter;

$query = alter()->materializedView('my_matview')
    ->setTablespace('fast_storage');

echo $query->toSql();
// ALTER MATERIALIZED VIEW my_matview SET TABLESPACE fast_storage

// With IF EXISTS
$query = alter()->materializedView('my_matview')
    ->ifExists()
    ->setTablespace('fast_storage');

echo $query->toSql();
// ALTER MATERIALIZED VIEW IF EXISTS my_matview SET TABLESPACE fast_storage
```

## DROP VIEW

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->view('active_users');

echo $query->toSql();
// DROP VIEW active_users
```

### Multiple Views

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->view('view1', 'view2', 'view3');

echo $query->toSql();
// DROP VIEW view1, view2, view3
```

### IF EXISTS

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->view('active_users')
    ->ifExists();

echo $query->toSql();
// DROP VIEW IF EXISTS active_users
```

### CASCADE

Drop objects that depend on the view:

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->view('active_users')
    ->cascade();

echo $query->toSql();
// DROP VIEW active_users CASCADE
```

### Combined Options

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->view('active_users')
    ->ifExists()
    ->cascade();

echo $query->toSql();
// DROP VIEW IF EXISTS active_users CASCADE
```

## DROP MATERIALIZED VIEW

### Simple Drop

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->materializedView('user_stats');

echo $query->toSql();
// DROP MATERIALIZED VIEW user_stats
```

### With Options

```php
<?php

use function Flow\PostgreSql\DSL\drop;

$query = drop()->materializedView('user_stats')
    ->ifExists()
    ->cascade();

echo $query->toSql();
// DROP MATERIALIZED VIEW IF EXISTS user_stats CASCADE
```

## REFRESH MATERIALIZED VIEW

### Simple Refresh

Repopulate a materialized view with current data:

```php
<?php

use function Flow\PostgreSql\DSL\refresh_materialized_view;

$query = refresh_materialized_view('user_stats');

echo $query->toSql();
// REFRESH MATERIALIZED VIEW user_stats
```

### With Schema

```php
<?php

use function Flow\PostgreSql\DSL\refresh_materialized_view;

$query = refresh_materialized_view('analytics.user_stats');

echo $query->toSql();
// REFRESH MATERIALIZED VIEW analytics.user_stats
```

### CONCURRENTLY

Refresh without locking out concurrent selects (requires a unique index on the view):

```php
<?php

use function Flow\PostgreSql\DSL\refresh_materialized_view;

$query = refresh_materialized_view('user_stats')
    ->concurrently();

echo $query->toSql();
// REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats
```

### WITH DATA / WITH NO DATA

```php
<?php

use function Flow\PostgreSql\DSL\refresh_materialized_view;

// Populate with data (default)
$query = refresh_materialized_view('user_stats')
    ->withData();

echo $query->toSql();
// REFRESH MATERIALIZED VIEW user_stats WITH DATA

// Empty the view
$query = refresh_materialized_view('user_stats')
    ->withNoData();

echo $query->toSql();
// REFRESH MATERIALIZED VIEW user_stats WITH NO DATA
```

### Complete Example

```php
<?php

use function Flow\PostgreSql\DSL\refresh_materialized_view;

$query = refresh_materialized_view('user_stats')
    ->concurrently()
    ->withData();

echo $query->toSql();
// REFRESH MATERIALIZED VIEW CONCURRENTLY user_stats WITH DATA
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
