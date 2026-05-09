# Condition Builder

[DOC_LINK:/documentation/components/libs/postgresql.md]

[TOC]

The Condition Builder provides a fluent API for building WHERE conditions incrementally. It's particularly useful when conditions need to be constructed dynamically based on runtime logic, such as building filter queries from user input.

## Basic Usage

```php
<?php

use function Flow\PostgreSql\DSL\{
    conditions, select, table, col, eq, gt, param, literal
};

// Create a condition builder and add conditions fluently
$conditions = conditions()
    ->and(eq(col('status'), literal('active')))
    ->and(gt(col('age'), literal(18)));

// Use in a query
$query = select()->from(table('users'))->where($conditions);

echo $query->toSql();
// SELECT * FROM users WHERE status = 'active' AND age > 18
```

## Building Conditions Incrementally

The real power of `ConditionBuilder` is building conditions based on runtime logic:

```php
<?php

use function Flow\PostgreSql\DSL\{
    conditions, select, table, col, eq, ge, like, param
};

function buildUserQuery(array $filters): SelectFinalStep
{
    $conditions = conditions();

    if (array_key_exists('status', $filters)) {
        $conditions = $conditions->and(eq(col('status'), param($filters['status'])));
    }

    if (array_key_exists('min_age', $filters)) {
        $conditions = $conditions->and(ge(col('age'), param($filters['min_age'])));
    }

    if (array_key_exists('email_domain', $filters)) {
        $conditions = $conditions->and(like(col('email'), param('%@' . $filters['email_domain'])));
    }

    $query = select()->from(table('users'));

    // Only add WHERE clause if we have conditions
    if (!$conditions->isEmpty()) {
        $query = $query->where($conditions);
    }

    return $query;
}

// Usage
$query = buildUserQuery(['status' => 'active', 'min_age' => 21]);
echo $query->toSql();
// SELECT * FROM users WHERE status = $1 AND age >= $2
```

## Nested Conditions

Build complex nested conditions with OR groups inside AND:

```php
<?php

use function Flow\PostgreSql\DSL\{
    conditions, select, table, col, eq, gt, literal
};

// WHERE status = 'active' AND (role = 'admin' OR role = 'moderator') AND age > 18
$conditions = conditions()
    ->and(eq(col('status'), literal('active')))
    ->and(
        conditions()
            ->or(eq(col('role'), literal('admin')))
            ->or(eq(col('role'), literal('moderator')))
    )
    ->and(gt(col('age'), literal(18)));

$query = select()->from(table('users'))->where($conditions);

echo $query->toSql();
// SELECT * FROM users WHERE status = 'active' AND (role = 'admin' OR role = 'moderator') AND age > 18
```

## Immutability

`ConditionBuilder` is immutable - each `and()` or `or()` call returns a new builder instance:

```php
<?php

use function Flow\PostgreSql\DSL\{conditions, eq, gt, col, literal};

$base = conditions()->and(eq(col('active'), literal(true)));

// Each branch creates a new builder
$withRole = $base->and(eq(col('role'), literal('admin')));
$withAge = $base->and(gt(col('age'), literal(18)));

// $base is unchanged
```

## Empty Builder Behavior

- `isEmpty()` returns `true` when no conditions have been added
- `getCondition()` returns `null` for empty builders
- Passing an empty builder to `where()` throws `InvalidBuilderStateException`
- Empty nested builders are ignored (no-op) when combined with `and()` or `or()`

```php
<?php

use function Flow\PostgreSql\DSL\{conditions, eq, col, literal};

$conditions = conditions();

// Safe pattern: check before using
if (!$conditions->isEmpty()) {
    $query = $query->where($conditions);
}

// Empty nested builders are safely ignored
$conditions = conditions()
    ->and(eq(col('status'), literal('active')))
    ->and(conditions()); // Empty conditions is ignored

// Result: only "status = 'active'" condition
```

## Method Reference

| Method | Description |
|--------|-------------|
| `conditions()` | DSL function - creates a new empty `ConditionBuilder` |
| `and(Condition\|ConditionBuilder $condition)` | Add a condition with AND logic |
| `or(Condition\|ConditionBuilder $condition)` | Add a condition with OR logic |
| `getCondition()` | Returns the built `Condition` or `null` if empty |
| `isEmpty()` | Returns `true` if no conditions have been added |

## Comparison with and_/or_

For static conditions known at build time, `and_()` and `or_()` are simpler:

```php
// Static conditions - use and_/or_
$query = select()
    ->from(table('users'))
    ->where(and_(
        eq(col('active'), literal(true)),
        gt(col('age'), literal(18))
    ));

// Dynamic conditions - use ConditionBuilder
$conditions = conditions();
foreach ($filters as $filter) {
    $conditions = $conditions->and($filter->toCondition());
}
```
