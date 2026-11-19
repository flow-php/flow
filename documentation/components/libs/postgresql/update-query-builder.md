# Update Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Update Query Builder provides a fluent, type-safe interface for constructing PostgreSQL UPDATE queries. It supports simple updates, updates with FROM clause (join-like behavior), complex WHERE conditions, and RETURNING clauses.

## Simple Update

```php
<?php

use function Flow\PostgreSql\DSL\{update, literal, col, eq};

$query = update()
    ->update('users')
    ->set('name', literal('John'))
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// UPDATE users SET name = 'John' WHERE id = 1
```

## Update with Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PostgreSql\DSL\{update, param, col, eq};

$query = update()
    ->update('users')
    ->set('name', param(1))
    ->where(eq(col('id'), param(2)));

echo $query->toSql();
// UPDATE users SET name = $1 WHERE id = $2
```

## Multiple SET Clauses

```php
<?php

use function Flow\PostgreSql\DSL\{update, literal, col, eq};

// Chained set() calls
$query = update()
    ->update('users')
    ->set('name', literal('John'))
    ->set('email', literal('john@example.com'))
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1

// Or use setAll() for multiple columns at once
$query = update()
    ->update('users')
    ->setAll([
        'name' => literal('John'),
        'email' => literal('john@example.com'),
    ])
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1
```

## Update with Table Alias

```php
<?php

use function Flow\PostgreSql\DSL\{update, literal, col, eq};

$query = update()
    ->update('users', 'u')
    ->set('name', literal('John'))
    ->where(eq(col('u.id'), literal(1)));

echo $query->toSql();
// UPDATE users u SET name = 'John' WHERE u.id = 1
```

## Update with FROM Clause (Join-like)

The FROM clause allows you to reference other tables in your UPDATE, similar to a JOIN:

```php
<?php

use function Flow\PostgreSql\DSL\{update, literal, table, col, eq};

$query = update()
    ->update('orders')
    ->set('status', literal('completed'))
    ->from(table('users'))
    ->where(eq(col('orders.user_id'), col('users.id')));

echo $query->toSql();
// UPDATE orders SET status = 'completed' FROM users WHERE orders.user_id = users.id
```

## Update with Subquery in SET

```php
<?php

use function Flow\PostgreSql\DSL\{
    update, select, sub_select, table, col, eq, literal
};

$subquery = select()
    ->select(col('avg_price'))
    ->from(table('price_stats'))
    ->where(eq(col('category'), col('products.category')));

$query = update()
    ->update('products')
    ->set('price', sub_select($subquery))
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// UPDATE products SET price = (SELECT avg_price FROM price_stats WHERE category = products.category) WHERE id = 1
```

## Update Column with Another Column

```php
<?php

use function Flow\PostgreSql\DSL\{update, col, eq, literal};

$query = update()
    ->update('products')
    ->set('price', col('original_price'))
    ->where(eq(col('id'), literal(1)));

echo $query->toSql();
// UPDATE products SET price = original_price WHERE id = 1
```

## Update with CTE

```php
<?php

use function Flow\PostgreSql\DSL\{with, cte, delete, select, col, table, eq, literal, in_subquery};

$query = with(
    cte('moved', delete()->from('children')->where(eq(col('parent_id'), literal(1)))->returning(col('id')))
)
    ->update()
    ->update('audit')
    ->set('note', literal('moved'))
    ->where(in_subquery(col('id'), select(col('id'))->from(table('moved'))));

echo $query->toSql();
// WITH moved AS (DELETE FROM children WHERE parent_id = 1 RETURNING id) UPDATE audit SET note = 'moved' WHERE id IN (SELECT id FROM moved)
```

## RETURNING Clause

```php
<?php

use function Flow\PostgreSql\DSL\{update, literal, col, eq};

// Return specific columns
$query = update()
    ->update('users')
    ->set('name', literal('John'))
    ->where(eq(col('id'), literal(1)))
    ->returning(col('id'), col('name'));

echo $query->toSql();
// UPDATE users SET name = 'John' WHERE id = 1 RETURNING id, name

// Return all columns
$query = update()
    ->update('users')
    ->set('name', literal('John'))
    ->where(eq(col('id'), literal(1)))
    ->returningAll();

echo $query->toSql();
// UPDATE users SET name = 'John' WHERE id = 1 RETURNING *

// Old and new values through renamed aliases
$query = update()
    ->update('children')
    ->set('parent_id', literal(2))
    ->where(eq(col('id'), literal(10)))
    ->returningAliases(old: 'o', new: 'n')
    ->returning(col('parent_id', 'o'), col('parent_id', 'n'));

echo $query->toSql();
// UPDATE children SET parent_id = 2 WHERE id = 10 RETURNING WITH (OLD AS o, NEW AS n) o.parent_id, n.parent_id
```

## Complex WHERE Conditions

```php
<?php

use function Flow\PostgreSql\DSL\{
    update, literal, literal,
    col, eq, gt, and_
};

$query = update()
    ->update('users')
    ->set('status', literal('premium'))
    ->where(
        and_(
            eq(col('active'), literal(true)),
            gt(col('orders_count'), literal(100))
        )
    );

echo $query->toSql();
// UPDATE users SET status = 'premium' WHERE active = true AND orders_count > 100
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
