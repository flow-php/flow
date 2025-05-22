# SQL Query Builder

Advanced SQL Query Builder for Flow PHP ETL with support for Common Table Expressions (CTEs) and LATERAL JOINs.

## Installation

```bash
composer require flow-php/sql-query-builder
```

## Features

- **Common Table Expressions (CTE)** - Build complex queries with WITH clauses
- **LATERAL JOINs** - Support for correlated subqueries  
- **Query Parts Exposure** - Full access to query components for programmatic modification
- **Platform Support** - MySQL, PostgreSQL, and SQLite dialects
- **Parameter Management** - Type-safe parameter binding
- **Backward Compatibility** - Adapter for existing DBAL QueryBuilder

## Usage

```php
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;

$builder = new FlowQueryBuilder();

$sql = $builder
    ->with('recent_orders', function($qb) {
        return $qb->select('*')
            ->from('orders')
            ->where('created_at > :date')
            ->setParameter('date', new \DateTime('-30 days'));
    })
    ->select('u.id', 'u.name', 'COUNT(ro.id) as order_count')
    ->from('users', 'u')
    ->leftJoin('u', 'recent_orders', 'ro', 'ro.user_id = u.id')
    ->groupBy('u.id', 'u.name')
    ->toSQL();
```

## Requirements

- PHP 8.2+
- Doctrine DBAL 3.8+ or 4.0+