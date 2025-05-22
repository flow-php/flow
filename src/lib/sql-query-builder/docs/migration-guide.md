# Migration Guide: From DBAL QueryBuilder to Flow QueryBuilder

This guide helps you migrate from Doctrine DBAL QueryBuilder to Flow SQL QueryBuilder to take advantage of advanced SQL features like CTEs and LATERAL JOINs.

## Table of Contents
1. [Overview](#overview)
2. [Quick Start](#quick-start)
3. [Feature Comparison](#feature-comparison)
4. [Migration Strategies](#migration-strategies)
5. [Code Examples](#code-examples)
6. [Common Issues](#common-issues)

## Overview

Flow SQL QueryBuilder extends DBAL QueryBuilder capabilities with:
- ✅ Common Table Expressions (WITH clauses)
- ✅ Recursive CTEs
- ✅ LATERAL JOINs (PostgreSQL, MySQL 8.0.14+)
- ✅ Query Parts exposure for programmatic modification
- ✅ Platform-specific SQL generation
- ✅ Full backward compatibility

## Quick Start

### Installation

```bash
composer require flow-php/sql-query-builder
```

### Basic Usage

```php
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

// Create from DBAL connection
$platform = PlatformFactory::fromConnection($connection);
$builder = new FlowQueryBuilder($platform);

// Or use the wrapper for drop-in replacement
use Flow\ETL\SQL\QueryBuilder\Adapter\FlowQueryBuilderWrapper;

$builder = new FlowQueryBuilderWrapper($connection);
// Now you can use both DBAL methods and new features
```

## Feature Comparison

| Feature | DBAL QueryBuilder | Flow QueryBuilder |
|---------|------------------|-------------------|
| Basic SELECT/FROM/WHERE | ✅ | ✅ |
| JOINs | ✅ | ✅ |
| GROUP BY/HAVING | ✅ | ✅ |
| ORDER BY | ✅ | ✅ |
| LIMIT/OFFSET | ✅ | ✅ |
| Parameter binding | ✅ | ✅ |
| Expression builder | ✅ | ✅ |
| Common Table Expressions | ❌ | ✅ |
| Recursive CTEs | ❌ | ✅ |
| LATERAL JOINs | ❌ | ✅ |
| Query Parts access | Limited | ✅ Full |
| Platform detection | ✅ | ✅ |

## Migration Strategies

### Strategy 1: Drop-in Replacement (Recommended)

Use `FlowQueryBuilderWrapper` for immediate compatibility:

```php
// Before
$qb = $connection->createQueryBuilder();

// After
use Flow\ETL\SQL\QueryBuilder\Adapter\FlowQueryBuilderWrapper;
$qb = new FlowQueryBuilderWrapper($connection);
```

### Strategy 2: Gradual Migration

Convert existing QueryBuilder instances when needed:

```php
use Flow\ETL\SQL\QueryBuilder\Adapter\DbalQueryBuilderAdapter;

// Convert existing DBAL QueryBuilder
$dbalQb = $connection->createQueryBuilder();
// ... configure DBAL query ...

$flowQb = DbalQueryBuilderAdapter::fromDbalQueryBuilder($dbalQb);
```

### Strategy 3: New Development

Use Flow QueryBuilder directly for new code:

```php
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

$platform = PlatformFactory::fromConnection($connection);
$qb = new FlowQueryBuilder($platform);
```

## Code Examples

### Using CTEs (Common Table Expressions)

```php
// Calculate monthly sales with CTEs
$monthlyStats = $qb->clone()
    ->select('DATE_FORMAT(order_date, "%Y-%m") as month', 'SUM(total) as revenue')
    ->from('orders')
    ->groupBy('month');

$builder = new FlowQueryBuilder($platform);
$sql = $builder
    ->with('monthly_sales', $monthlyStats)
    ->select('ms.month', 'ms.revenue', 'LAG(ms.revenue) OVER (ORDER BY ms.month) as prev_revenue')
    ->from('monthly_sales', 'ms')
    ->toSQL();
```

### Using Recursive CTEs

```php
// Build category hierarchy
$builder = new FlowQueryBuilder($platform);

$initial = 'SELECT id, name, parent_id, 0 as level FROM categories WHERE parent_id IS NULL';
$recursive = 'SELECT c.id, c.name, c.parent_id, ct.level + 1 FROM categories c JOIN category_tree ct ON c.parent_id = ct.id';

$sql = $builder
    ->withRecursive('category_tree', $initial, $recursive, ['id', 'name', 'parent_id', 'level'])
    ->select('*')
    ->from('category_tree')
    ->orderBy('level')
    ->toSQL();
```

### Using LATERAL JOINs

```php
// Get latest 5 orders for each user
$recentOrders = $builder->clone()
    ->select('*')
    ->from('orders', 'o')
    ->where('o.user_id = u.id')
    ->orderBy('o.created_at', 'DESC')
    ->limit(5);

$sql = $builder
    ->select('u.id', 'u.name', 'recent.*')
    ->from('users', 'u')
    ->leftLateralJoin('u', $recentOrders, 'recent')
    ->toSQL();
```

### Programmatic Query Modification

```php
// Access and modify query parts
$queryParts = $builder->getQueryParts();

// Reset specific parts
$builder->resetQueryPart('orderBy');
$builder->resetQueryPart('groupBy');

// Clone and modify
$countQuery = $builder->clone()
    ->select('COUNT(*)')
    ->resetQueryPart('orderBy');
```

## Common Issues

### 1. Platform Detection

Flow QueryBuilder auto-detects the platform from DBAL connection:

```php
// Automatic platform detection
$platform = PlatformFactory::fromConnection($connection);

// Or specify manually
$platform = PlatformFactory::create('postgresql');
```

### 2. Parameter Types

Flow QueryBuilder maintains DBAL parameter types:

```php
$builder->setParameter('date', new \DateTime(), Types::DATETIME_MUTABLE);
$builder->setParameter('id', 123, Types::INTEGER);
```

### 3. Expression Builder Compatibility

Flow's expression builder is compatible with DBAL:

```php
$expr = $builder->expr();

// Same API as DBAL
$condition = $expr->andX(
    $expr->eq('status', ':status'),
    $expr->gte('created_at', ':date')
);

$builder->where($condition);
```

### 4. Extractor Compatibility

Flow ETL extractors work with both builders:

```php
// Works with DBAL QueryBuilder
$extractor = new DbalLimitOffsetExtractor($connection, $dbalQueryBuilder);

// Also works with Flow QueryBuilder
$extractor = new DbalLimitOffsetExtractor($connection, $flowQueryBuilder);
```

## Advanced Features

### Platform-Specific Features

```php
// Check platform capabilities
if ($platform->supportsLateralJoins()) {
    $builder->lateralJoin(...);
}

if ($platform->supportsRecursiveCTE()) {
    $builder->withRecursive(...);
}
```

### Custom Platform Implementation

```php
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformInterface;

class CustomPlatform implements PlatformInterface
{
    // Implement platform-specific SQL generation
}
```

## Performance Considerations

1. **Query Cloning**: Use `clone()` method for efficient query copying
2. **Parameter Merging**: CTEs and subqueries automatically merge parameters
3. **Platform Caching**: Platform detection is cached per connection

## Troubleshooting

### Error: "SQLite does not support LATERAL joins"

SQLite doesn't support LATERAL. Use alternative approaches:

```php
// Instead of LATERAL JOIN, use correlated subquery
$builder->select(
    'u.*',
    '(SELECT COUNT(*) FROM orders WHERE user_id = u.id) as order_count'
)->from('users', 'u');
```

### Error: "Unknown query part"

Ensure you use correct part names:
- DBAL: `join`, `groupBy`, `orderBy`
- Flow: `joins`, `groupBy`, `orderBy`

The FlowQueryBuilderWrapper handles this mapping automatically.

## Support

- GitHub Issues: [flow-php/flow](https://github.com/flow-php/flow/issues)
- Documentation: [Flow PHP Docs](https://github.com/flow-php/flow/documentation)