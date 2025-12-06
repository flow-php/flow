# Select Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Select Query Builder provides a fluent, type-safe interface for constructing PostgreSQL SELECT queries. It guides you through building valid queries step-by-step, from simple selects to complex queries with JOINs, CTEs, and window functions.

## Simple Select

```php
<?php

use function Flow\PgQuery\DSL\{select, col_from_string, star, table};

// Select specific columns
$query = select()
    ->select(col_from_string('id'), col_from_string('name'))
    ->from(table('users'));

echo $query->toSQL();
// SELECT id, name FROM users

// Select all columns
$query = select()
    ->select(star())
    ->from(table('users'));

echo $query->toSQL();
// SELECT * FROM users
```

## WHERE Clause

```php
<?php

use function Flow\PgQuery\DSL\{
    select, star, table, col_from_string,
    eq, gt, lt, gte, lte, neq, between, is_in, like, is_null,
    literal_string, literal_int, literal_bool,
    cond_and, cond_or, cond_not
};

// Simple condition
$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(eq(col_from_string('active'), literal_bool(true)));

echo $query->toSQL();
// SELECT * FROM users WHERE active = true

// Multiple conditions with AND
$query = select()
    ->select(star())
    ->from(table('products'))
    ->where(
        cond_and(
            gt(col_from_string('price'), literal_int(10)),
            lt(col_from_string('price'), literal_int(100)),
            eq(col_from_string('in_stock'), literal_bool(true))
        )
    );

echo $query->toSQL();
// SELECT * FROM products WHERE price > 10 AND price < 100 AND in_stock = true

// Complex conditions with AND/OR
$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(
        cond_and(
            eq(col_from_string('status'), literal_string('active')),
            cond_or(
                gte(col_from_string('age'), literal_int(18)),
                eq(col_from_string('guardian_approved'), literal_bool(true))
            )
        )
    );

echo $query->toSQL();
// SELECT * FROM users WHERE status = 'active' AND (age >= 18 OR guardian_approved = true)

// BETWEEN condition
$query = select()
    ->select(star())
    ->from(table('products'))
    ->where(between(col_from_string('price'), literal_int(10), literal_int(100)));

echo $query->toSQL();
// SELECT * FROM products WHERE price BETWEEN 10 AND 100

// IN condition
$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(is_in(col_from_string('status'), [
        literal_string('active'),
        literal_string('pending'),
        literal_string('verified')
    ]));

echo $query->toSQL();
// SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')

// LIKE condition
$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(like(col_from_string('email'), literal_string('%@example.com')));

echo $query->toSQL();
// SELECT * FROM users WHERE email LIKE '%@example.com'
```

## JOINs

```php
<?php

use function Flow\PgQuery\DSL\{
    select, star, col_from_string, table, eq
};

// INNER JOIN
$query = select()
    ->select(col_from_string('u.name'), col_from_string('o.total'))
    ->from(table('users')->as('u'))
    ->join(
        table('orders')->as('o'),
        eq(col_from_string('u.id'), col_from_string('o.user_id'))
    );

echo $query->toSQL();
// SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id

// LEFT JOIN
$query = select()
    ->select(col_from_string('u.name'), col_from_string('o.total'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        table('orders')->as('o'),
        eq(col_from_string('u.id'), col_from_string('o.user_id'))
    );

echo $query->toSQL();
// SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id

// RIGHT JOIN
$query = select()
    ->select(star())
    ->from(table('orders')->as('o'))
    ->rightJoin(
        table('users')->as('u'),
        eq(col_from_string('o.user_id'), col_from_string('u.id'))
    );

echo $query->toSQL();
// SELECT * FROM orders o RIGHT JOIN users u ON o.user_id = u.id

// FULL JOIN
$query = select()
    ->select(star())
    ->from(table('table_a')->as('a'))
    ->fullJoin(
        table('table_b')->as('b'),
        eq(col_from_string('a.key'), col_from_string('b.key'))
    );

echo $query->toSQL();
// SELECT * FROM table_a a FULL JOIN table_b b ON a.key = b.key

// CROSS JOIN
$query = select()
    ->select(star())
    ->from(table('colors'))
    ->crossJoin(table('sizes'));

echo $query->toSQL();
// SELECT * FROM colors CROSS JOIN sizes
```

## ORDER BY Clause

```php
<?php

use function Flow\PgQuery\DSL\{
    select, star, table, col_from_string, asc, desc, order_by
};

use Flow\PgQuery\QueryBuilder\Clause\{SortDirection, NullsPosition};

// Simple ORDER BY
$query = select()
    ->select(star())
    ->from(table('users'))
    ->orderBy(asc(col_from_string('name')));

echo $query->toSQL();
// SELECT * FROM users ORDER BY name ASC

// Multiple columns
$query = select()
    ->select(star())
    ->from(table('users'))
    ->orderBy(
        asc(col_from_string('last_name')),
        desc(col_from_string('first_name'))
    );

echo $query->toSQL();
// SELECT * FROM users ORDER BY last_name ASC, first_name DESC

// NULLS FIRST / NULLS LAST
$query = select()
    ->select(star())
    ->from(table('products'))
    ->orderBy(
        order_by(col_from_string('price'), SortDirection::ASC, NullsPosition::FIRST),
        order_by(col_from_string('name'), SortDirection::DESC, NullsPosition::LAST)
    );

echo $query->toSQL();
// SELECT * FROM products ORDER BY price ASC NULLS FIRST, name DESC NULLS LAST
```

## LIMIT and OFFSET

```php
<?php

use function Flow\PgQuery\DSL\{select, star, table, col_from_string, asc};

$query = select()
    ->select(star())
    ->from(table('users'))
    ->orderBy(asc(col_from_string('id')))
    ->limit(10)
    ->offset(20);

echo $query->toSQL();
// SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20
```

## Common Table Expressions (CTE)

```php
<?php

use function Flow\PgQuery\DSL\{
    select, select_with, star, table, col_from_string,
    cte, cte_ref, with_cte, eq, literal_bool
};

use Flow\PgQuery\QueryBuilder\Clause\CTEMaterialization;

// Simple CTE
$query = select_with(with_cte([
    cte(
        'active_users',
        select()
            ->select(col_from_string('id'), col_from_string('name'))
            ->from(table('users'))
            ->where(eq(col_from_string('active'), literal_bool(true)))
    ),
]))
    ->select(star())
    ->from(cte_ref('active_users'));

echo $query->toSQL();
// WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users

// Materialized CTE
$query = select_with(with_cte([
    cte(
        'active_users',
        select()
            ->select(col_from_string('id'), col_from_string('name'))
            ->from(table('users'))
            ->where(eq(col_from_string('active'), literal_bool(true))),
        [],
        CTEMaterialization::MATERIALIZED
    ),
]))
    ->select(star())
    ->from(cte_ref('active_users'));

echo $query->toSQL();
// WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users
```

## Derived Tables (Subqueries in FROM)

```php
<?php

use function Flow\PgQuery\DSL\{
    select, table, col_from_string, derived, agg_sum, eq
};

$subquery = select()
    ->select(col_from_string('user_id'), agg_sum(col_from_string('amount'))->as('total'))
    ->from(table('orders'))
    ->groupBy(col_from_string('user_id'));

$query = select()
    ->select(col_from_string('u.name'), col_from_string('o.total'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        derived($subquery, 'o'),
        eq(col_from_string('u.id'), col_from_string('o.user_id'))
    );

echo $query->toSQL();
// SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id
```

## LATERAL Join

```php
<?php

use function Flow\PgQuery\DSL\{
    select, star, table, col_from_string, derived, lateral, desc, eq, raw_cond
};

$lateralQuery = select()
    ->select(star())
    ->from(table('orders'))
    ->where(eq(col_from_string('orders.user_id'), col_from_string('u.id')))
    ->orderBy(desc(col_from_string('created_at')))
    ->limit(3);

$query = select()
    ->select(col_from_string('u.name'), col_from_string('recent.id'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        lateral(derived($lateralQuery, 'recent')),
        raw_cond('true')
    );

echo $query->toSQL();
// SELECT u.name, recent.id FROM users u LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = u.id ORDER BY created_at DESC LIMIT 3) recent ON true
```

## JSONB Contains Operator (@>)

For JSONB operations like the `@>` (contains) operator, use `raw_expr()` or `raw_cond()`:

```php
<?php

use function Flow\PgQuery\DSL\{select, star, table, raw_cond};

$query = select()
    ->select(star())
    ->from(table('products'))
    ->where(raw_cond("metadata @> '{\"category\": \"electronics\"}'"));

echo $query->toSQL();
// SELECT * FROM products WHERE metadata @> '{"category": "electronics"}'
```

## Aggregates and GROUP BY

```php
<?php

use function Flow\PgQuery\DSL\{
    select, table, col_from_string,
    agg_count, agg_sum, agg_avg, agg_min, agg_max, gt, literal_int
};

// Simple aggregates
$query = select()
    ->select(
        col_from_string('category'),
        agg_count(),
        agg_sum(col_from_string('amount')),
        agg_avg(col_from_string('price')),
        agg_min(col_from_string('created_at')),
        agg_max(col_from_string('updated_at'))
    )
    ->from(table('orders'))
    ->groupBy(col_from_string('category'));

echo $query->toSQL();
// SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category

// GROUP BY with HAVING
$query = select()
    ->select(col_from_string('category'), agg_count()->as('cnt'))
    ->from(table('products'))
    ->groupBy(col_from_string('category'))
    ->having(gt(agg_count(), literal_int(5)));

echo $query->toSQL();
// SELECT category, count(*) AS cnt FROM products GROUP BY category HAVING count(*) > 5

// COUNT DISTINCT
$query = select()
    ->select(agg_count(col_from_string('user_id'), true)->as('unique_users'))
    ->from(table('orders'));

echo $query->toSQL();
// SELECT count(DISTINCT user_id) AS unique_users FROM orders
```

## UNION, INTERSECT, EXCEPT

```php
<?php

use function Flow\PgQuery\DSL\{select, col_from_string, table};

$query1 = select()
    ->select(col_from_string('name'))
    ->from(table('users'));

$query2 = select()
    ->select(col_from_string('name'))
    ->from(table('admins'));

// UNION
$query = $query1->union($query2);

echo $query->toSQL();
// SELECT name FROM users UNION SELECT name FROM admins

// UNION ALL
$query = $query1->unionAll($query2);

echo $query->toSQL();
// SELECT name FROM users UNION ALL SELECT name FROM admins

// INTERSECT
$query = $query1->intersect($query2);

echo $query->toSQL();
// SELECT name FROM users INTERSECT SELECT name FROM admins

// EXCEPT
$query = $query1->except($query2);

echo $query->toSQL();
// SELECT name FROM users EXCEPT SELECT name FROM admins
```

## Subqueries

```php
<?php

use function Flow\PgQuery\DSL\{
    select, star, table, col_from_string,
    sub_select, exists, agg_count, eq, literal_int
};

// Scalar subquery
$subquery = select()
    ->select(agg_count())
    ->from(table('orders'))
    ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

$query = select()
    ->select(
        col_from_string('name'),
        sub_select($subquery)->as('order_count')
    )
    ->from(table('users'));

echo $query->toSQL();
// SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users

// EXISTS subquery
$subquery = select()
    ->select(literal_int(1))
    ->from(table('orders'))
    ->where(eq(col_from_string('orders.user_id'), col_from_string('users.id')));

$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(exists($subquery));

echo $query->toSQL();
// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

## Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PgQuery\DSL\{select, star, table, col_from_string, eq, param};

$query = select()
    ->select(star())
    ->from(table('users'))
    ->where(eq(col_from_string('id'), param(1)));

echo $query->toSQL();
// SELECT * FROM users WHERE id = $1
```

## FOR UPDATE / FOR SHARE

```php
<?php

use function Flow\PgQuery\DSL\{select, star, table, col_from_string, eq, literal_int};

$query = select()
    ->select(star())
    ->from(table('accounts'))
    ->where(eq(col_from_string('id'), literal_int(1)))
    ->forUpdate();

echo $query->toSQL();
// SELECT * FROM accounts WHERE id = 1 FOR UPDATE

$query = select()
    ->select(star())
    ->from(table('accounts'))
    ->where(eq(col_from_string('id'), literal_int(1)))
    ->forShare();

echo $query->toSQL();
// SELECT * FROM accounts WHERE id = 1 FOR SHARE
```

## Column Aliases

```php
<?php

use function Flow\PgQuery\DSL\{select, col_from_string, table};

$query = select()
    ->select(
        col_from_string('first_name')->as('fname'),
        col_from_string('last_name')->as('lname')
    )
    ->from(table('users'));

echo $query->toSQL();
// SELECT first_name AS fname, last_name AS lname FROM users
```

## DISTINCT

```php
<?php

use function Flow\PgQuery\DSL\{select, col_from_string, table, asc, desc};

// SELECT DISTINCT
$query = select()
    ->selectDistinct(col_from_string('city'))
    ->from(table('users'));

echo $query->toSQL();
// SELECT DISTINCT city FROM users

// DISTINCT ON
$query = select()
    ->selectDistinctOn(
        [col_from_string('department')],
        col_from_string('name'),
        col_from_string('salary')
    )
    ->from(table('employees'))
    ->orderBy(asc(col_from_string('department')), desc(col_from_string('salary')));

echo $query->toSQL();
// SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department ASC, salary DESC
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
