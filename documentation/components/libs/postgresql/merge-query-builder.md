# Merge Query Builder

- [⬅️ Back](/documentation/components/libs/pg-query.md)

[TOC]

The Merge Query Builder provides a fluent, type-safe interface for constructing PostgreSQL MERGE queries. MERGE (also known as "upsert" in some contexts) allows you to perform INSERT, UPDATE, or DELETE operations in a single statement based on whether rows match between a source and target table.

## Simple Merge with Update

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('target_table', 't')
    ->using('source_table', 's')
    ->on(eq(col('t.id'), col('s.id')))
    ->whenMatched()
    ->thenUpdate([
        'name' => col('s.name'),
        'value' => col('s.value'),
    ]);

echo $query->toSQL();
// MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name, value = s.value
```

## Merge with Delete

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('target_table', 't')
    ->using('source_table', 's')
    ->on(eq(col('t.id'), col('s.id')))
    ->whenMatched()
    ->thenDelete();

echo $query->toSQL();
// MERGE INTO target_table t USING source_table s ON t.id = s.id WHEN MATCHED THEN DELETE
```

## Merge with Insert

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('customers')
    ->using('new_customers', 'nc')
    ->on(eq(col('customers.id'), col('nc.id')))
    ->whenNotMatched()
    ->thenInsert(
        ['id', 'name', 'email'],
        [col('nc.id'), col('nc.name'), col('nc.email')]
    );

echo $query->toSQL();
// MERGE INTO customers USING new_customers nc ON customers.id = nc.id WHEN NOT MATCHED THEN INSERT (id, name, email) VALUES (nc.id, nc.name, nc.email)
```

## Merge with Insert Values

Alternative syntax using column => value pairs:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq, literal};

$query = merge('users')
    ->using('new_users', 'n')
    ->on(eq(col('users.id'), col('n.id')))
    ->whenNotMatched()
    ->thenInsertValues([
        'id' => col('n.id'),
        'name' => col('n.name'),
        'status' => literal('active'),
    ]);

echo $query->toSQL();
// MERGE INTO users USING new_users n ON users.id = n.id WHEN NOT MATCHED THEN INSERT (id, name, status) VALUES (n.id, n.name, 'active')
```

## Multiple WHEN Clauses

Combine different actions based on matching conditions:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('inventory', 'i')
    ->using('updates', 'u')
    ->on(eq(col('i.product_id'), col('u.product_id')))
    ->whenMatched()
    ->thenUpdate([
        'quantity' => col('u.quantity'),
        'updated_at' => col('u.updated_at'),
    ])
    ->whenNotMatched()
    ->thenInsert(
        ['product_id', 'quantity'],
        [col('u.product_id'), col('u.quantity')]
    );

echo $query->toSQL();
// MERGE INTO inventory i USING updates u ON i.product_id = u.product_id WHEN MATCHED THEN UPDATE SET quantity = u.quantity, updated_at = u.updated_at WHEN NOT MATCHED THEN INSERT (product_id, quantity) VALUES (u.product_id, u.quantity)
```

## Conditional WHEN Clauses

Add conditions to WHEN MATCHED and WHEN NOT MATCHED clauses:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq, gt, literal};

$query = merge('products', 'p')
    ->using('price_updates', 'pu')
    ->on(eq(col('p.id'), col('pu.product_id')))
    ->whenMatchedAnd(gt(col('pu.price'), literal(0)))
    ->thenUpdate([
        'price' => col('pu.price'),
    ]);

echo $query->toSQL();
// MERGE INTO products p USING price_updates pu ON p.id = pu.product_id WHEN MATCHED AND pu.price > 0 THEN UPDATE SET price = pu.price
```

## DO NOTHING Action

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('products')
    ->using('updates', 'u')
    ->on(eq(col('products.id'), col('u.id')))
    ->whenMatched()
    ->thenDoNothing();

echo $query->toSQL();
// MERGE INTO products USING updates u ON products.id = u.id WHEN MATCHED THEN DO NOTHING
```

## Merge Using Subquery

Use a subquery as the source:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq, select, cte_ref};

$sourceQuery = select()
    ->select(col('id'), col('name'), col('email'))
    ->from(cte_ref('staged_data'));

$query = merge('users')
    ->using($sourceQuery, 'src')
    ->on(eq(col('users.id'), col('src.id')))
    ->whenMatched()
    ->thenUpdate([
        'name' => col('src.name'),
        'email' => col('src.email'),
    ]);

echo $query->toSQL();
// MERGE INTO users USING (SELECT id, name, email FROM staged_data) src ON users.id = src.id WHEN MATCHED THEN UPDATE SET name = src.name, email = src.email
```

## Merge with CTE (WITH Clause)

```php
<?php

use function Flow\PgQuery\DSL\{with, col, eq, select, cte, cte_ref, with_cte};

$stagedData = select()
    ->select(col('id'), col('name'))
    ->from(cte_ref('raw_input'));

$withClause = with_cte([
    cte('staged_data', $stagedData),
]);

$query = with($withClause)->merge('users')
    ->using('staged_data', 's')
    ->on(eq(col('users.id'), col('s.id')))
    ->whenMatched()
    ->thenUpdate([
        'name' => col('s.name'),
    ]);

echo $query->toSQL();
// WITH staged_data AS (SELECT id, name FROM raw_input) MERGE INTO users USING staged_data s ON users.id = s.id WHEN MATCHED THEN UPDATE SET name = s.name
```

## Merge with Parameters

Use positional parameters for prepared statements:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq, param};

$query = merge('accounts')
    ->using('transactions', 't')
    ->on(eq(col('accounts.id'), col('t.account_id')))
    ->whenMatched()
    ->thenUpdate([
        'balance' => param(1),
    ]);

echo $query->toSQL();
// MERGE INTO accounts USING transactions t ON accounts.id = t.account_id WHEN MATCHED THEN UPDATE SET balance = $1
```

## WHEN NOT MATCHED BY SOURCE

Handle rows in the target that don't have matching rows in the source:

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('target', 't')
    ->using('source', 's')
    ->on(eq(col('t.id'), col('s.id')))
    ->whenNotMatchedBySource()
    ->thenDelete();

echo $query->toSQL();
// MERGE INTO target t USING source s ON t.id = s.id WHEN NOT MATCHED BY SOURCE THEN DELETE
```

## Schema-Qualified Tables

```php
<?php

use function Flow\PgQuery\DSL\{merge, col, eq};

$query = merge('myschema.users', 'u')
    ->using('public.updates', 's')
    ->on(eq(col('u.id'), col('s.id')))
    ->whenMatched()
    ->thenDoNothing();

echo $query->toSQL();
// MERGE INTO myschema.users u USING public.updates s ON u.id = s.id WHEN MATCHED THEN DO NOTHING
```

## DSL Functions

For a complete list of DSL functions, see the [DSL reference](/documentation/api/lib/pg-query/namespaces/flow-pgquery-dsl.html).
