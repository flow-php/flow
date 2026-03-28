# Delete Query Builder

- [⬅️ Back](/documentation/components/libs/postgresql.md)

[TOC]

The Delete Query Builder provides a fluent, type-safe interface for constructing PostgreSQL DELETE queries. It supports simple deletes, deletes with USING clause (join-like behavior), complex WHERE conditions, and RETURNING clauses.

## Simple Delete

```php
<?php

use function Flow\PostgreSql\DSL\{delete, col, eq, literal};

$query = delete()
    ->from('users')
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// DELETE FROM users WHERE id = 1
```

## Delete with Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PostgreSql\DSL\{delete, col, eq, param};

$query = delete()
    ->from('users')
    ->where(eq(col('id'), param(1)));

echo $query->toSql();
// DELETE FROM users WHERE id = $1
```

## Delete with Table Alias

```php
<?php

use function Flow\PostgreSql\DSL\{delete, col, eq, literal};

$query = delete()
    ->from('users', 'u')
    ->where(eq(col('u.id'), literal(1)));

echo $query->toSql();
// DELETE FROM users u WHERE u.id = 1
```

## Delete with USING Clause (Join-like)

The USING clause allows you to reference other tables in your DELETE, similar to a JOIN:

```php
<?php

use function Flow\PostgreSql\DSL\{delete, table, col, eq};

$query = delete()
    ->from('orders')
    ->using(table('users'))
    ->where(eq(col('orders.user_id'), col('users.id')));

echo $query->toSql();
// DELETE FROM orders USING users WHERE orders.user_id = users.id
```

## Delete with Subquery in WHERE

```php
<?php

use function Flow\PostgreSql\DSL\{
    delete, select, col, table, any_
};

use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;

$subquery = select()
    ->select(col('user_id'))
    ->from(table('inactive_users'));

$query = delete()
    ->from('users')
    ->where(any_(col('id'), ComparisonOperator::EQ, $subquery));

echo $query->toSql();
// DELETE FROM users WHERE id = ANY (SELECT user_id FROM inactive_users)
```

## RETURNING Clause

```php
<?php

use function Flow\PostgreSql\DSL\{delete, col, eq, literal};

// Return specific columns
$query = delete()
    ->from('users')
    ->where(eq(col('id'), literal(1)))
    ->returning(col('id'), col('name'));

echo $query->toSql();
// DELETE FROM users WHERE id = 1 RETURNING id, name

// Return all columns
$query = delete()
    ->from('users')
    ->where(eq(col('id'), literal(1)))
    ->returningAll();

echo $query->toSql();
// DELETE FROM users WHERE id = 1 RETURNING *
```

## Complex WHERE Conditions

```php
<?php

use function Flow\PostgreSql\DSL\{
    delete, col, eq, lt, literal, and_
};

$query = delete()
    ->from('sessions')
    ->where(
        and_(
            eq(col('active'), literal(false)),
            lt(col('expires_at'), literal('2024-01-01'))
        )
    );

echo $query->toSql();
// DELETE FROM sessions WHERE active = false AND expires_at < '2024-01-01'
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
