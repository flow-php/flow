# Insert Query Builder

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The Insert Query Builder provides a fluent, type-safe interface for constructing PostgreSQL INSERT queries. It supports simple inserts, multi-row inserts, INSERT ... SELECT, upserts (ON CONFLICT), and RETURNING clauses.

## Simple Insert

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal};

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(literal('John'), literal('john@example.com'));

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ('John', 'john@example.com')
```

## Insert with Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PostgreSql\DSL\{insert, param};

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(param(1), param(2));

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ($1, $2)
```

## Multi-Row Insert

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal};

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(literal('John'), literal('john@example.com'))
    ->values(literal('Jane'), literal('jane@example.com'));

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ('John', 'john@example.com'), ('Jane', 'jane@example.com')
```

## Insert with Default Values

```php
<?php

use function Flow\PostgreSql\DSL\insert;

$query = insert()
    ->into('users')
    ->defaultValues();

echo $query->toSQL();
// INSERT INTO users DEFAULT VALUES
```

## INSERT ... SELECT

```php
<?php

use function Flow\PostgreSql\DSL\{insert, select, table, col};

$selectQuery = select()
    ->select(col('name'), col('email'))
    ->from(table('archived_users'));

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->select($selectQuery);

echo $query->toSQL();
// INSERT INTO users (name, email) SELECT name, email FROM archived_users
```

## Upsert (ON CONFLICT)

### ON CONFLICT DO NOTHING

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal};

// Without specifying conflict target
$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(literal('John'), literal('john@example.com'))
    ->onConflictDoNothing();

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT DO NOTHING
```

### ON CONFLICT (columns) DO NOTHING

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal, conflict_columns};

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(literal('John'), literal('john@example.com'))
    ->onConflictDoNothing(conflict_columns(['email']));

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT (email) DO NOTHING
```

### ON CONFLICT ON CONSTRAINT DO NOTHING

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal, conflict_constraint};

$query = insert()
    ->into('users')
    ->columns('name', 'email')
    ->values(literal('John'), literal('john@example.com'))
    ->onConflictDoNothing(conflict_constraint('users_pkey'));

echo $query->toSQL();
// INSERT INTO users (name, email) VALUES ('John', 'john@example.com') ON CONFLICT ON CONSTRAINT users_pkey DO NOTHING
```

### ON CONFLICT DO UPDATE

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal, conflict_columns};

$query = insert()
    ->into('users')
    ->columns('email', 'name')
    ->values(literal('john@example.com'), literal('John'))
    ->onConflictDoUpdate(
        conflict_columns(['email']),
        ['name' => literal('Updated John')]
    );

echo $query->toSQL();
// INSERT INTO users (email, name) VALUES ('john@example.com', 'John') ON CONFLICT (email) DO UPDATE SET name = 'Updated John'
```

### ON CONFLICT DO UPDATE with EXCLUDED

Reference the values that would have been inserted using the `excluded` pseudo-table:

```php
<?php

use function Flow\PostgreSql\DSL\{insert, param, conflict_columns, col};

$query = insert()
    ->into('users')
    ->columns('email', 'name')
    ->values(param(1), param(2))
    ->onConflictDoUpdate(
        conflict_columns(['email']),
        ['name' => col('excluded.name')]
    );

echo $query->toSQL();
// INSERT INTO users (email, name) VALUES ($1, $2) ON CONFLICT (email) DO UPDATE SET name = excluded.name
```

### ON CONFLICT DO UPDATE with WHERE

```php
<?php

use function Flow\PostgreSql\DSL\{
    insert, literal, conflict_columns, eq, col
};

$query = insert()
    ->into('users')
    ->columns('email', 'name', 'active')
    ->values(literal('john@example.com'), literal('John'), literal(true))
    ->onConflictDoUpdate(
        conflict_columns(['email']),
        ['name' => literal('Updated John')]
    )
    ->where(eq(col('users.active'), literal(true)));

echo $query->toSQL();
// INSERT INTO users (email, name, active) VALUES ('john@example.com', 'John', true) ON CONFLICT (email) DO UPDATE SET name = 'Updated John' WHERE users.active = true
```

## RETURNING Clause

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal, col};

// Return specific columns
$query = insert()
    ->into('users')
    ->columns('name')
    ->values(literal('John'))
    ->returning(col('id'));

echo $query->toSQL();
// INSERT INTO users (name) VALUES ('John') RETURNING id

// Return all columns
$query = insert()
    ->into('users')
    ->columns('name')
    ->values(literal('John'))
    ->returningAll();

echo $query->toSQL();
// INSERT INTO users (name) VALUES ('John') RETURNING *
```

## Schema-Qualified Tables

```php
<?php

use function Flow\PostgreSql\DSL\{insert, literal};

$query = insert()
    ->into('public.users')
    ->columns('name')
    ->values(literal('John'));

echo $query->toSQL();
// INSERT INTO public.users (name) VALUES ('John')
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
