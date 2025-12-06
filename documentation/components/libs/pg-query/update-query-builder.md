# Update Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Update Query Builder provides a fluent, type-safe interface for constructing PostgreSQL UPDATE queries. It supports simple updates, updates with FROM clause (join-like behavior), complex WHERE conditions, and RETURNING clauses.

## Simple Update

```php
<?php

use function Flow\PgQuery\DSL\{update, literal_string, col_from_string, eq, literal_int};

$query = update()
    ->update('users')
    ->set('name', literal_string('John'))
    ->where(eq(col_from_string('id'), literal_int(1)));

echo $query->toSQL();
// UPDATE users SET name = 'John' WHERE id = 1
```

## Update with Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PgQuery\DSL\{update, param, col_from_string, eq};

$query = update()
    ->update('users')
    ->set('name', param(1))
    ->where(eq(col_from_string('id'), param(2)));

echo $query->toSQL();
// UPDATE users SET name = $1 WHERE id = $2
```

## Multiple SET Clauses

```php
<?php

use function Flow\PgQuery\DSL\{update, literal_string, col_from_string, eq, literal_int};

// Chained set() calls
$query = update()
    ->update('users')
    ->set('name', literal_string('John'))
    ->set('email', literal_string('john@example.com'))
    ->where(eq(col_from_string('id'), literal_int(1)));

echo $query->toSQL();
// UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1

// Or use setAll() for multiple columns at once
$query = update()
    ->update('users')
    ->setAll([
        'name' => literal_string('John'),
        'email' => literal_string('john@example.com'),
    ])
    ->where(eq(col_from_string('id'), literal_int(1)));

echo $query->toSQL();
// UPDATE users SET name = 'John', email = 'john@example.com' WHERE id = 1
```

## Update with Table Alias

```php
<?php

use function Flow\PgQuery\DSL\{update, literal_string, col_from_string, eq, literal_int};

$query = update()
    ->update('users', 'u')
    ->set('name', literal_string('John'))
    ->where(eq(col_from_string('u.id'), literal_int(1)));

echo $query->toSQL();
// UPDATE users u SET name = 'John' WHERE u.id = 1
```

## Update with FROM Clause (Join-like)

The FROM clause allows you to reference other tables in your UPDATE, similar to a JOIN:

```php
<?php

use function Flow\PgQuery\DSL\{update, literal_string, table, col_from_string, eq};

$query = update()
    ->update('orders')
    ->set('status', literal_string('completed'))
    ->from(table('users'))
    ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

echo $query->toSQL();
// UPDATE orders SET status = 'completed' FROM users WHERE orders.user_id = users.id
```

## Update with Subquery in SET

```php
<?php

use function Flow\PgQuery\DSL\{
    update, select, sub_select, table, col_from_string, eq, literal_int
};

$subquery = select()
    ->select(col_from_string('avg_price'))
    ->from(table('price_stats'))
    ->where(eq(col_from_string('category'), col_from_string('products.category')));

$query = update()
    ->update('products')
    ->set('price', sub_select($subquery))
    ->where(eq(col_from_string('id'), literal_int(1)));

echo $query->toSQL();
// UPDATE products SET price = (SELECT avg_price FROM price_stats WHERE category = products.category) WHERE id = 1
```

## Update Column with Another Column

```php
<?php

use function Flow\PgQuery\DSL\{update, col_from_string, eq, literal_int};

$query = update()
    ->update('products')
    ->set('price', col_from_string('original_price'))
    ->where(eq(col_from_string('id'), literal_int(1)));

echo $query->toSQL();
// UPDATE products SET price = original_price WHERE id = 1
```

## RETURNING Clause

```php
<?php

use function Flow\PgQuery\DSL\{update, literal_string, col_from_string, eq, literal_int};

// Return specific columns
$query = update()
    ->update('users')
    ->set('name', literal_string('John'))
    ->where(eq(col_from_string('id'), literal_int(1)))
    ->returning(col_from_string('id'), col_from_string('name'));

echo $query->toSQL();
// UPDATE users SET name = 'John' WHERE id = 1 RETURNING id, name

// Return all columns
$query = update()
    ->update('users')
    ->set('name', literal_string('John'))
    ->where(eq(col_from_string('id'), literal_int(1)))
    ->returningAll();

echo $query->toSQL();
// UPDATE users SET name = 'John' WHERE id = 1 RETURNING *
```

## Complex WHERE Conditions

```php
<?php

use function Flow\PgQuery\DSL\{
    update, literal_string, literal_int, literal_bool,
    col_from_string, eq, gt, cond_and
};

$query = update()
    ->update('users')
    ->set('status', literal_string('premium'))
    ->where(
        cond_and(
            eq(col_from_string('active'), literal_bool(true)),
            gt(col_from_string('orders_count'), literal_int(100))
        )
    );

echo $query->toSQL();
// UPDATE users SET status = 'premium' WHERE active = true AND orders_count > 100
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
