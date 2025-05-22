<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Unit;

use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\GenericPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\MySQLPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\PostgreSQLPlatform;
use Flow\ETL\SQL\QueryBuilder\Platform\SQLitePlatform;
use PHPUnit\Framework\TestCase;

class FlowQueryBuilderTest extends TestCase
{
    public function test_basic_select_query(): void
    {
        $builder = new FlowQueryBuilder();
        
        $sql = $builder
            ->select('id', 'name', 'email')
            ->from('users', 'u')
            ->where('u.active = :active')
            ->setParameter('active', true)
            ->toSQL();

        $expected = <<<SQL
SELECT id, name, email
FROM "users" AS "u"
WHERE u.active = :active
SQL;

        self::assertEquals($expected, $sql);
        self::assertEquals(['active' => true], $builder->getParameters());
    }

    public function test_query_with_joins(): void
    {
        $builder = new FlowQueryBuilder();
        
        $sql = $builder
            ->select('u.id', 'u.name', 'COUNT(o.id) as order_count')
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->groupBy('u.id', 'u.name')
            ->having('COUNT(o.id) > :min_orders')
            ->setParameter('min_orders', 5)
            ->toSQL();

        $expected = <<<SQL
SELECT u.id, u.name, COUNT(o.id) as order_count
FROM "users" AS "u"
LEFT JOIN "orders" AS "o" ON o.user_id = u.id
WHERE 1 = 1
GROUP BY "u"."id", "u"."name"
HAVING COUNT(o.id) > :min_orders
SQL;

        self::assertStringContainsString('LEFT JOIN "orders" AS "o"', $sql);
        self::assertStringContainsString('GROUP BY', $sql);
        self::assertStringContainsString('HAVING COUNT(o.id) > :min_orders', $sql);
    }

    public function test_query_with_cte(): void
    {
        $builder = new FlowQueryBuilder(new PostgreSQLPlatform());
        
        $cteBuilder = new FlowQueryBuilder(new PostgreSQLPlatform());
        $cteQuery = $cteBuilder
            ->select('*')
            ->from('orders')
            ->where('created_at > :date')
            ->setParameter('date', '2024-01-01');
        
        $sql = $builder
            ->with('recent_orders', $cteQuery)
            ->select('u.id', 'u.name', 'COUNT(ro.id) as order_count')
            ->from('users', 'u')
            ->leftJoin('u', 'recent_orders', 'ro', 'ro.user_id = u.id')
            ->groupBy('u.id', 'u.name')
            ->toSQL();

        self::assertStringStartsWith('WITH "recent_orders" AS', $sql);
        self::assertStringContainsString('SELECT *', $sql);
        self::assertStringContainsString('FROM "orders"', $sql);
        self::assertStringContainsString('WHERE created_at > :date', $sql);
        
        // Check parameters are merged
        self::assertEquals(['date' => '2024-01-01'], $builder->getParameters());
    }

    public function test_recursive_cte(): void
    {
        $builder = new FlowQueryBuilder();
        
        $initial = 'SELECT id, parent_id, name FROM categories WHERE parent_id IS NULL';
        $recursive = 'SELECT c.id, c.parent_id, c.name FROM categories c JOIN category_tree ct ON c.parent_id = ct.id';
        
        $sql = $builder
            ->withRecursive('category_tree', $initial, $recursive, ['id', 'parent_id', 'name'])
            ->select('*')
            ->from('category_tree')
            ->toSQL();

        self::assertStringStartsWith('WITH RECURSIVE "category_tree"', $sql);
        self::assertStringContainsString('("id", "parent_id", "name") AS', $sql);
        self::assertStringContainsString('UNION ALL', $sql);
    }

    public function test_lateral_join_postgresql(): void
    {
        $builder = new FlowQueryBuilder(new PostgreSQLPlatform());
        
        $lateralSubquery = new FlowQueryBuilder(new PostgreSQLPlatform());
        $lateralSubquery
            ->select('*')
            ->from('orders', 'o')
            ->where('o.user_id = u.id')
            ->orderBy('o.created_at', 'DESC')
            ->limit(5);
        
        $sql = $builder
            ->select('u.id', 'u.name', 'recent.*')
            ->from('users', 'u')
            ->leftLateralJoin('u', $lateralSubquery, 'recent')
            ->toSQL();

        self::assertStringContainsString('LEFT JOIN LATERAL', $sql);
        self::assertStringContainsString('AS "recent" ON TRUE', $sql);
    }

    public function test_lateral_join_not_supported_sqlite(): void
    {
        $builder = new FlowQueryBuilder(new SQLitePlatform());
        
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('SQLite does not support LATERAL joins');
        
        $builder
            ->select('*')
            ->from('users', 'u')
            ->lateralJoin('u', 'SELECT * FROM orders WHERE user_id = u.id', 'o')
            ->toSQL();
    }

    public function test_expression_builder(): void
    {
        $builder = new FlowQueryBuilder();
        $expr = $builder->expr();
        
        $sql = $builder
            ->select('*')
            ->from('users')
            ->where($expr->andX(
                $expr->eq('status', ':status'),
                $expr->gte('age', ':min_age'),
                $expr->in('role', [':role1', ':role2'])
            ))
            ->toSQL();

        self::assertStringContainsString('"status" = :status', $sql);
        self::assertStringContainsString('"age" >= :min_age', $sql);
        self::assertStringContainsString('"role" IN (:role1, :role2)', $sql);
    }

    public function test_query_parts_access(): void
    {
        $builder = new FlowQueryBuilder();
        
        $builder
            ->select('id', 'name')
            ->from('users', 'u')
            ->where('active = 1')
            ->orderBy('name')
            ->limit(10)
            ->offset(20);

        $parts = $builder->getQueryParts();
        
        self::assertEquals(['id', 'name'], $parts['select']);
        self::assertEquals(['table' => 'users', 'alias' => 'u'], $parts['from']);
        self::assertEquals(['active = 1'], $parts['where']);
        self::assertEquals([['column' => 'name', 'direction' => 'ASC']], $parts['orderBy']);
        self::assertEquals(10, $parts['limit']);
        self::assertEquals(20, $parts['offset']);
    }

    public function test_reset_query_part(): void
    {
        $builder = new FlowQueryBuilder();
        
        $builder
            ->select('*')
            ->from('users')
            ->where('active = 1')
            ->orderBy('name');

        $builder->resetQueryPart('where');
        $builder->resetQueryPart('orderBy');
        
        $parts = $builder->getQueryParts();
        
        self::assertEmpty($parts['where']);
        self::assertEmpty($parts['orderBy']);
    }

    public function test_clone_builder(): void
    {
        $builder = new FlowQueryBuilder();
        
        $builder
            ->select('*')
            ->from('users')
            ->where('active = :active')
            ->setParameter('active', true);

        $clone = $builder->clone();
        $clone->andWhere('role = :role')->setParameter('role', 'admin');
        
        // Original should not be affected
        self::assertCount(1, $builder->getQueryParts()['where']);
        self::assertCount(1, $builder->getParameters());
        
        // Clone should have additional conditions
        self::assertCount(2, $clone->getQueryParts()['where']);
        self::assertCount(2, $clone->getParameters());
    }

    public function test_mysql_limit_offset_syntax(): void
    {
        $builder = new FlowQueryBuilder(new MySQLPlatform());
        
        $sql = $builder
            ->select('*')
            ->from('users')
            ->limit(10)
            ->offset(20)
            ->toSQL();

        // MySQL uses LIMIT offset, count syntax
        self::assertStringContainsString('LIMIT 20, 10', $sql);
        self::assertStringNotContainsString('OFFSET', $sql);
    }

    public function test_platform_specific_identifiers(): void
    {
        // PostgreSQL uses double quotes
        $pgBuilder = new FlowQueryBuilder(new PostgreSQLPlatform());
        $pgSql = $pgBuilder->select('*')->from('users')->toSQL();
        self::assertStringContainsString('"users"', $pgSql);
        
        // MySQL uses backticks
        $mysqlBuilder = new FlowQueryBuilder(new MySQLPlatform());
        $mysqlSql = $mysqlBuilder->select('*')->from('users')->toSQL();
        self::assertStringContainsString('`users`', $mysqlSql);
        
        // SQLite can use multiple styles, we use double quotes
        $sqliteBuilder = new FlowQueryBuilder(new SQLitePlatform());
        $sqliteSql = $sqliteBuilder->select('*')->from('users')->toSQL();
        self::assertStringContainsString('"users"', $sqliteSql);
    }

    public function test_complex_where_conditions(): void
    {
        $builder = new FlowQueryBuilder();
        
        $builder
            ->select('*')
            ->from('users')
            ->where('active = 1')
            ->andWhere('created_at > :date')
            ->orWhere('role = :admin_role')
            ->setParameter('date', '2024-01-01')
            ->setParameter('admin_role', 'admin');

        $sql = $builder->toSQL();
        
        self::assertStringContainsString('(active = 1 AND created_at > :date) OR (role = :admin_role)', $sql);
    }

    public function test_parameter_types(): void
    {
        $builder = new FlowQueryBuilder();
        
        $builder
            ->select('*')
            ->from('users')
            ->where('id = :id')
            ->setParameter('id', 123, 'integer')
            ->setParameter('name', 'John', 'string');

        $types = $builder->getParameterTypes();
        
        self::assertEquals('integer', $types['id']);
        self::assertEquals('string', $types['name']);
    }

    public function test_empty_in_expression(): void
    {
        $builder = new FlowQueryBuilder();
        $expr = $builder->expr();
        
        // Empty IN should return always false
        self::assertEquals('1 = 0', $expr->in('id', []));
        
        // Empty NOT IN should return always true
        self::assertEquals('1 = 1', $expr->notIn('id', []));
    }
}