# Select Query Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Select Query Builder provides a fluent, type-safe interface for constructing PostgreSQL SELECT queries. It guides you through building valid queries step-by-step, from simple selects to complex queries with JOINs, CTEs, and window functions.

## Simple Select

```php
<?php

use function Flow\PostgreSql\DSL\{select, col, star, table};

// Select specific columns
$query = select(col('id'), col('name'))
    ->from(table('users'));

echo $query->toSql();
// SELECT id, name FROM users

// Select all columns
$query = select(star())
    ->from(table('users'));

echo $query->toSql();
// SELECT * FROM users
```

## WHERE Clause

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col,
    eq, gt, lt, ge, le, ne, between, in_, like, is_null,
    literal,
    and_, or_, not_
};

// Simple condition
$query = select(star())
    ->from(table('users'))
    ->where(eq(col('active'), literal(true)));

echo $query->toSql();
// SELECT * FROM users WHERE active = true

// Multiple conditions with AND
$query = select(star())
    ->from(table('products'))
    ->where(
        and_(
            gt(col('price'), literal(10)),
            lt(col('price'), literal(100)),
            eq(col('in_stock'), literal(true))
        )
    );

echo $query->toSql();
// SELECT * FROM products WHERE price > 10 AND price < 100 AND in_stock = true

// Complex conditions with AND/OR
$query = select(star())
    ->from(table('users'))
    ->where(
        and_(
            eq(col('status'), literal('active')),
            or_(
                ge(col('age'), literal(18)),
                eq(col('guardian_approved'), literal(true))
            )
        )
    );

echo $query->toSql();
// SELECT * FROM users WHERE status = 'active' AND (age >= 18 OR guardian_approved = true)

// BETWEEN condition
$query = select(star())
    ->from(table('products'))
    ->where(between(col('price'), literal(10), literal(100)));

echo $query->toSql();
// SELECT * FROM products WHERE price BETWEEN 10 AND 100

// IN condition
$query = select(star())
    ->from(table('users'))
    ->where(in_(col('status'), [
        literal('active'),
        literal('pending'),
        literal('verified')
    ]));

echo $query->toSql();
// SELECT * FROM users WHERE status IN ('active', 'pending', 'verified')

// LIKE condition
$query = select(star())
    ->from(table('users'))
    ->where(like(col('email'), literal('%@example.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email LIKE '%@example.com'
```

> **Tip:** For building WHERE conditions dynamically based on runtime logic (e.g., user filters), see [Condition Builder](/documentation/components/libs/postgresql/condition-builder.md).

## JOINs

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, col, table, eq
};

// INNER JOIN
$query = select(col('u.name'), col('o.total'))
    ->from(table('users')->as('u'))
    ->join(
        table('orders')->as('o'),
        eq(col('u.id'), col('o.user_id'))
    );

echo $query->toSql();
// SELECT u.name, o.total FROM users u JOIN orders o ON u.id = o.user_id

// LEFT JOIN
$query = select(col('u.name'), col('o.total'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        table('orders')->as('o'),
        eq(col('u.id'), col('o.user_id'))
    );

echo $query->toSql();
// SELECT u.name, o.total FROM users u LEFT JOIN orders o ON u.id = o.user_id

// RIGHT JOIN
$query = select(star())
    ->from(table('orders')->as('o'))
    ->rightJoin(
        table('users')->as('u'),
        eq(col('o.user_id'), col('u.id'))
    );

echo $query->toSql();
// SELECT * FROM orders o RIGHT JOIN users u ON o.user_id = u.id

// FULL JOIN
$query = select(star())
    ->from(table('table_a')->as('a'))
    ->fullJoin(
        table('table_b')->as('b'),
        eq(col('a.key'), col('b.key'))
    );

echo $query->toSql();
// SELECT * FROM table_a a FULL JOIN table_b b ON a.key = b.key

// CROSS JOIN
$query = select(star())
    ->from(table('colors'))
    ->crossJoin(table('sizes'));

echo $query->toSql();
// SELECT * FROM colors CROSS JOIN sizes
```

## ORDER BY Clause

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, asc, desc, order_by
};

use Flow\PostgreSql\QueryBuilder\Clause\{SortDirection, NullsPosition};

// Simple ORDER BY
$query = select(star())
    ->from(table('users'))
    ->orderBy(asc(col('name')));

echo $query->toSql();
// SELECT * FROM users ORDER BY name ASC

// Multiple columns
$query = select(star())
    ->from(table('users'))
    ->orderBy(
        asc(col('last_name')),
        desc(col('first_name'))
    );

echo $query->toSql();
// SELECT * FROM users ORDER BY last_name ASC, first_name DESC

// NULLS FIRST / NULLS LAST
$query = select(star())
    ->from(table('products'))
    ->orderBy(
        order_by(col('price'), SortDirection::ASC, NullsPosition::FIRST),
        order_by(col('name'), SortDirection::DESC, NullsPosition::LAST)
    );

echo $query->toSql();
// SELECT * FROM products ORDER BY price ASC NULLS FIRST, name DESC NULLS LAST
```

## LIMIT and OFFSET

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, asc};

$query = select(star())
    ->from(table('users'))
    ->orderBy(asc(col('id')))
    ->limit(10)
    ->offset(20);

echo $query->toSql();
// SELECT * FROM users ORDER BY id ASC LIMIT 10 OFFSET 20
```

## Common Table Expressions (CTE)

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, with, star, table, col, cte, eq, literal
};

use Flow\PostgreSql\QueryBuilder\Clause\CTEMaterialization;

// Simple CTE
$query = with(
    cte(
        'active_users',
        select(col('id'), col('name'))
            ->from(table('users'))
            ->where(eq(col('active'), literal(true)))
    )
)
    ->select(star())
    ->from(table('active_users'));

echo $query->toSql();
// WITH active_users AS (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users

// Multiple CTEs
$query = with(
    cte('users_cte', select(col('id'), col('name'))->from(table('users'))),
    cte('orders_cte', select(col('id'), col('user_id'))->from(table('orders')))
)
    ->select(star())
    ->from(table('users_cte'));

echo $query->toSql();
// WITH users_cte AS (SELECT id, name FROM users), orders_cte AS (SELECT id, user_id FROM orders) SELECT * FROM users_cte

// Recursive CTE
$query = with(cte('tree', $recursiveQuery))
    ->recursive()
    ->select(star())
    ->from(table('tree'));

echo $query->toSql();
// WITH RECURSIVE tree AS (...) SELECT * FROM tree

// Materialized CTE
$query = with(
    cte(
        'active_users',
        select(col('id'), col('name'))
            ->from(table('users'))
            ->where(eq(col('active'), literal(true))),
        [],
        CTEMaterialization::MATERIALIZED
    )
)
    ->select(star())
    ->from(table('active_users'));

echo $query->toSql();
// WITH active_users AS MATERIALIZED (SELECT id, name FROM users WHERE active = true) SELECT * FROM active_users
```

## Derived Tables (Subqueries in FROM)

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, table, col, derived, agg_sum, eq
};

$subquery = select(col('user_id'), agg_sum(col('amount'))->as('total'))
    ->from(table('orders'))
    ->groupBy(col('user_id'));

$query = select(col('u.name'), col('o.total'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        derived($subquery, 'o'),
        eq(col('u.id'), col('o.user_id'))
    );

echo $query->toSql();
// SELECT u.name, o.total FROM users u LEFT JOIN (SELECT user_id, sum(amount) AS total FROM orders GROUP BY user_id) o ON u.id = o.user_id
```

## LATERAL Join

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, derived, lateral, desc, eq, is_true, literal
};

$lateralQuery = select(star())
    ->from(table('orders'))
    ->where(eq(col('orders.user_id'), col('u.id')))
    ->orderBy(desc(col('created_at')))
    ->limit(3);

$query = select()
    ->select(col('u.name'), col('recent.id'))
    ->from(table('users')->as('u'))
    ->leftJoin(
        lateral(derived($lateralQuery, 'recent')),
        is_true(literal(true))
    );

echo $query->toSql();
// SELECT u.name, recent.id FROM users u LEFT JOIN LATERAL (SELECT * FROM orders WHERE orders.user_id = u.id ORDER BY created_at DESC LIMIT 3) recent ON true
```

## JSONB Operators

The query builder provides native support for PostgreSQL JSONB operators:

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, literal, array_expr,
    json_contains, json_contained_by, json_get, json_get_text,
    json_path, json_path_text, json_exists, json_exists_any, json_exists_all
};

// JSONB contains (@>)
$query = select(star())
    ->from(table('products'))
    ->where(json_contains(col('metadata'), literal('{"category": "electronics"}')));

echo $query->toSql();
// SELECT * FROM products WHERE metadata @> '{"category": "electronics"}'

// JSONB is contained by (<@)
$query = select(star())
    ->from(table('products'))
    ->where(json_contained_by(col('metadata'), literal('{"category": "electronics", "price": 100}')));

echo $query->toSql();
// SELECT * FROM products WHERE metadata <@ '{"category": "electronics", "price": 100}'

// JSON field access (->) - returns JSON
$query = select(json_get(col('metadata'), literal('category'))->as('category'))
    ->from(table('products'));

echo $query->toSql();
// SELECT metadata -> 'category' AS category FROM products

// JSON field access (->>) - returns text
$query = select(json_get_text(col('metadata'), literal('name'))->as('product_name'))
    ->from(table('products'));

echo $query->toSql();
// SELECT metadata ->> 'name' AS product_name FROM products

// JSON path access (#>) - returns JSON
$query = select(json_path(col('metadata'), literal('{category,name}'))->as('nested'))
    ->from(table('products'));

echo $query->toSql();
// SELECT metadata #> '{category,name}' AS nested FROM products

// JSON path access (#>>) - returns text
$query = select(json_path_text(col('metadata'), literal('{category,name}'))->as('nested_text'))
    ->from(table('products'));

echo $query->toSql();
// SELECT metadata #>> '{category,name}' AS nested_text FROM products

// Key exists (?)
$query = select(star())
    ->from(table('products'))
    ->where(json_exists(col('metadata'), literal('category')));

echo $query->toSql();
// SELECT * FROM products WHERE metadata ? 'category'

// Any key exists (?|)
$query = select(star())
    ->from(table('products'))
    ->where(json_exists_any(col('metadata'), array_expr([literal('category'), literal('name')])));

echo $query->toSql();
// SELECT * FROM products WHERE metadata ?| array['category', 'name']

// All keys exist (?&)
$query = select(star())
    ->from(table('products'))
    ->where(json_exists_all(col('metadata'), array_expr([literal('category'), literal('name')])));

echo $query->toSql();
// SELECT * FROM products WHERE metadata ?& array['category', 'name']
```

## Array Operators

PostgreSQL array operators are also supported:

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, literal, array_expr,
    array_contains, array_contained_by, array_overlap
};

// Array contains (@>)
$query = select(star())
    ->from(table('products'))
    ->where(array_contains(col('tags'), array_expr([literal('sale')])));

echo $query->toSql();
// SELECT * FROM products WHERE tags @> ARRAY['sale']

// Array is contained by (<@)
$query = select(star())
    ->from(table('products'))
    ->where(array_contained_by(col('tags'), array_expr([literal('sale'), literal('featured'), literal('new')])));

echo $query->toSql();
// SELECT * FROM products WHERE tags <@ ARRAY['sale', 'featured', 'new']

// Array overlap (&&)
$query = select(star())
    ->from(table('products'))
    ->where(array_overlap(col('tags'), array_expr([literal('sale'), literal('featured')])));

echo $query->toSql();
// SELECT * FROM products WHERE tags && ARRAY['sale', 'featured']
```

## Pattern Matching (Regex)

POSIX regex operators for pattern matching:

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, literal,
    regex_match, regex_imatch, not_regex_match, not_regex_imatch
};

// Case-sensitive regex match (~)
$query = select(star())
    ->from(table('users'))
    ->where(regex_match(col('email'), literal('.*@gmail\\.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email ~ '.*@gmail\.com'

// Case-insensitive regex match (~*)
$query = select(star())
    ->from(table('users'))
    ->where(regex_imatch(col('email'), literal('.*@gmail\\.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email ~* '.*@gmail\.com'

// Does not match (!~)
$query = select(star())
    ->from(table('users'))
    ->where(not_regex_match(col('email'), literal('.*@spam\\.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email !~ '.*@spam\.com'

// Does not match case-insensitive (!~*)
$query = select(star())
    ->from(table('users'))
    ->where(not_regex_imatch(col('email'), literal('.*@spam\\.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email !~* '.*@spam\.com'
```

## SIMILAR TO

SQL-standard pattern matching (like LIKE but supports full regex character classes):

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, literal, similar_to};

$query = select(star())
    ->from(table('users'))
    ->where(similar_to(col('email'), literal('%@(gmail|yahoo)\\.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email SIMILAR TO '%@(gmail|yahoo)\.com'
```

## NOT LIKE

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, literal, not_like};

$query = select(star())
    ->from(table('users'))
    ->where(not_like(col('name'), literal('pg_%')));

echo $query->toSql();
// SELECT * FROM users WHERE name NOT LIKE 'pg_%'
```

## IS DISTINCT FROM

NULL-safe comparison (treats NULL as a known value):

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, literal, distinct_from};

$query = select(star())
    ->from(table('users'))
    ->where(distinct_from(col('email'), literal('test@example.com')));

echo $query->toSql();
// SELECT * FROM users WHERE email IS DISTINCT FROM 'test@example.com'
```

## ALL Condition

Compare against all values from a subquery or array:

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col,
    all_
};
use Flow\PostgreSql\QueryBuilder\Condition\ComparisonOperator;

$query = select(star())
    ->from(table('products'))
    ->where(all_(col('price'), ComparisonOperator::GT, col('thresholds')));

echo $query->toSql();
// SELECT * FROM products WHERE price > ALL(thresholds)
```

## Full-Text Search

PostgreSQL full-text search operator:

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col, literal, func, text_search_match
};

$query = select(star())
    ->from(table('documents'))
    ->where(text_search_match(col('content'), func('to_tsquery', [literal('english'), literal('hello & world')])));

echo $query->toSql();
// SELECT * FROM documents WHERE content @@ to_tsquery('english', 'hello & world')
```

## Aggregates and GROUP BY

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, table, col,
    agg_count, agg_sum, agg_avg, agg_min, agg_max, gt, literal
};

// Simple aggregates
$query = select(
        col('category'),
        agg_count(),
        agg_sum(col('amount')),
        agg_avg(col('price')),
        agg_min(col('created_at')),
        agg_max(col('updated_at'))
    )
    ->from(table('orders'))
    ->groupBy(col('category'));

echo $query->toSql();
// SELECT category, count(*), sum(amount), avg(price), min(created_at), max(updated_at) FROM orders GROUP BY category

// GROUP BY with HAVING
$query = select(col('category'), agg_count()->as('cnt'))
    ->from(table('products'))
    ->groupBy(col('category'))
    ->having(gt(agg_count(), literal(5)));

echo $query->toSql();
// SELECT category, count(*) AS cnt FROM products GROUP BY category HAVING count(*) > 5

// COUNT DISTINCT
$query = select(agg_count(col('user_id'), true)->as('unique_users'))
    ->from(table('orders'));

echo $query->toSql();
// SELECT count(DISTINCT user_id) AS unique_users FROM orders
```

## UNION, INTERSECT, EXCEPT

```php
<?php

use function Flow\PostgreSql\DSL\{select, col, table};

$query1 = select(col('name'))
    ->from(table('users'));

$query2 = select(col('name'))
    ->from(table('admins'));

// UNION
$query = $query1->union($query2);

echo $query->toSql();
// SELECT name FROM users UNION SELECT name FROM admins

// UNION ALL
$query = $query1->unionAll($query2);

echo $query->toSql();
// SELECT name FROM users UNION ALL SELECT name FROM admins

// INTERSECT
$query = $query1->intersect($query2);

echo $query->toSql();
// SELECT name FROM users INTERSECT SELECT name FROM admins

// EXCEPT
$query = $query1->except($query2);

echo $query->toSql();
// SELECT name FROM users EXCEPT SELECT name FROM admins
```

## Subqueries

```php
<?php

use function Flow\PostgreSql\DSL\{
    select, star, table, col,
    sub_select, exists, agg_count, eq, literal
};

// Scalar subquery
$subquery = select(agg_count())
    ->from(table('orders'))
    ->where(eq(col('orders.user_id'), col('users.id')));

$query = select(
        col('name'),
        sub_select($subquery)->as('order_count')
    )
    ->from(table('users'));

echo $query->toSql();
// SELECT name, (SELECT count(*) FROM orders WHERE orders.user_id = users.id) AS order_count FROM users

// EXISTS subquery
$subquery = select(literal(1))
    ->from(table('orders'))
    ->where(eq(col('orders.user_id'), col('users.id')));

$query = select(star())
    ->from(table('users'))
    ->where(exists($subquery));

echo $query->toSql();
// SELECT * FROM users WHERE EXISTS (SELECT 1 FROM orders WHERE orders.user_id = users.id)
```

## Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, eq, param};

$query = select(star())
    ->from(table('users'))
    ->where(eq(col('id'), param(1)));

echo $query->toSql();
// SELECT * FROM users WHERE id = $1
```

## FOR UPDATE / FOR SHARE

```php
<?php

use function Flow\PostgreSql\DSL\{select, star, table, col, eq, literal};

$query = select(star())
    ->from(table('accounts'))
    ->where(eq(col('id'), literal(1)))
    ->forUpdate();

echo $query->toSql();
// SELECT * FROM accounts WHERE id = 1 FOR UPDATE

$query = select(star())
    ->from(table('accounts'))
    ->where(eq(col('id'), literal(1)))
    ->forShare();

echo $query->toSql();
// SELECT * FROM accounts WHERE id = 1 FOR SHARE
```

## Column Aliases

```php
<?php

use function Flow\PostgreSql\DSL\{select, col, table};

$query = select(
        col('first_name')->as('fname'),
        col('last_name')->as('lname')
    )
    ->from(table('users'));

echo $query->toSql();
// SELECT first_name AS fname, last_name AS lname FROM users
```

## DISTINCT

```php
<?php

use function Flow\PostgreSql\DSL\{select, col, table, asc, desc};

// SELECT DISTINCT
$query = select()
    ->selectDistinct(col('city'))
    ->from(table('users'));

echo $query->toSql();
// SELECT DISTINCT city FROM users

// DISTINCT ON
$query = select()
    ->selectDistinctOn(
        [col('department')],
        col('name'),
        col('salary')
    )
    ->from(table('employees'))
    ->orderBy(asc(col('department')), desc(col('salary')));

echo $query->toSql();
// SELECT DISTINCT ON (department) name, salary FROM employees ORDER BY department ASC, salary DESC
```

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/postgresql/namespaces/flow-postgresql-dsl.html).
