<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

class SQLiteIntegrationTest extends DatabaseTestCase
{
    protected function createConnection(): Connection
    {
        return DriverManager::getConnection([
            'driver' => 'pdo_sqlite',
            'memory' => true,
        ]);
    }

    protected function createTestSchema(): void
    {
        // SQLite specific schema adjustments
        $this->connection->executeStatement('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                email TEXT,
                status TEXT,
                created_at TEXT
            )
        ');

        $this->connection->executeStatement('
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                user_id INTEGER NOT NULL,
                amount REAL NOT NULL,
                status TEXT,
                created_at TEXT,
                FOREIGN KEY (user_id) REFERENCES users(id)
            )
        ');

        $this->connection->executeStatement('
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                parent_id INTEGER,
                FOREIGN KEY (parent_id) REFERENCES categories(id)
            )
        ');

        $this->insertTestData();
    }

    public function test_basic_query(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $sql = $builder
            ->select('u.id', 'u.name', 'COUNT(o.id) as order_count')
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->where('u.status = :status')
            ->groupBy('u.id', 'u.name')
            ->having('COUNT(o.id) > :min_orders')
            ->orderBy('order_count', 'DESC')
            ->setParameter('status', 'active')
            ->setParameter('min_orders', 0);

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(4, $result); // 4 active users
        self::assertEquals('Eve Wilson', $result[0]['name']);
        self::assertEquals(2, $result[0]['order_count']);
    }

    public function test_cte_support(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // Create CTE for users with orders
        $activeUsersBuilder = new FlowQueryBuilder($platform);
        $activeUsers = $activeUsersBuilder
            ->select('DISTINCT u.id', 'u.name')
            ->from('users', 'u')
            ->join('u', 'orders', 'o', 'o.user_id = u.id')
            ->where('o.status = :order_status')
            ->setParameter('order_status', 'completed');

        $sql = $builder
            ->with('active_users', $activeUsers)
            ->select('*')
            ->from('active_users')
            ->orderBy('name');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(5, $result);
        self::assertEquals('Alice Johnson', $result[0]['name']);
    }

    public function test_recursive_cte(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $initial = "SELECT id, name, parent_id, 0 as level FROM categories WHERE parent_id IS NULL";
        $recursive = "SELECT c.id, c.name, c.parent_id, ct.level + 1 FROM categories c JOIN category_tree ct ON c.parent_id = ct.id";

        $sql = $builder
            ->withRecursive('category_tree', $initial, $recursive)
            ->select('*')
            ->from('category_tree')
            ->orderBy('level')
            ->addOrderBy('name');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertGreaterThan(10, count($result));
        
        // Check hierarchy levels
        $electronics = array_filter($result, fn($row) => $row['name'] === 'Electronics');
        self::assertEquals(0, reset($electronics)['level']);
        
        $computers = array_filter($result, fn($row) => $row['name'] === 'Computers');
        self::assertEquals(1, reset($computers)['level']);
        
        $laptops = array_filter($result, fn($row) => $row['name'] === 'Laptops');
        self::assertEquals(2, reset($laptops)['level']);
    }

    public function test_lateral_join_not_supported(): void
    {
        $this->expectException(\RuntimeException::class);
        $this->expectExceptionMessage('SQLite does not support LATERAL joins');

        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $subquery = "SELECT * FROM orders WHERE user_id = u.id LIMIT 5";
        
        $builder
            ->select('*')
            ->from('users', 'u')
            ->lateralJoin('u', $subquery, 'o')
            ->toSQL();
    }

    public function test_pagination_with_limit_offset(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $baseQuery = $builder
            ->select('*')
            ->from('users')
            ->orderBy('created_at');

        // Page 1
        $page1 = $baseQuery->clone()->limit(2)->offset(0);
        $result1 = $this->connection->executeQuery(
            $page1->toSQL(),
            $page1->getParameters()
        )->fetchAllAssociative();

        self::assertCount(2, $result1);
        self::assertEquals('Alice Johnson', $result1[0]['name']);
        self::assertEquals('Bob Smith', $result1[1]['name']);

        // Page 2
        $page2 = $baseQuery->clone()->limit(2)->offset(2);
        $result2 = $this->connection->executeQuery(
            $page2->toSQL(),
            $page2->getParameters()
        )->fetchAllAssociative();

        self::assertCount(2, $result2);
        self::assertEquals('Charlie Brown', $result2[0]['name']);
        self::assertEquals('Diana Prince', $result2[1]['name']);
    }

    public function test_expression_builder(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);
        $expr = $builder->expr();

        $sql = $builder
            ->select('*')
            ->from('orders')
            ->where($expr->andX(
                $expr->between('amount', ':min_amount', ':max_amount'),
                $expr->in('status', ['completed', 'pending']),
                $expr->isNotNull('user_id')
            ))
            ->orderBy('amount', 'DESC')
            ->setParameter('min_amount', 100)
            ->setParameter('max_amount', 300);

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters()
        )->fetchAllAssociative();

        self::assertGreaterThan(0, count($result));
        foreach ($result as $row) {
            self::assertGreaterThanOrEqual(100, $row['amount']);
            self::assertLessThanOrEqual(300, $row['amount']);
            self::assertContains($row['status'], ['completed', 'pending']);
        }
    }

    public function test_query_parts_modification(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $builder
            ->select('u.*', 'COUNT(o.id) as order_count')
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->groupBy('u.id')
            ->orderBy('order_count', 'DESC');

        // Get query parts
        $parts = $builder->getQueryParts();
        self::assertArrayHasKey('select', $parts);
        self::assertArrayHasKey('joins', $parts);
        self::assertArrayHasKey('groupBy', $parts);
        self::assertArrayHasKey('orderBy', $parts);

        // Modify query by resetting parts
        $countQuery = $builder->clone()
            ->select('COUNT(DISTINCT u.id)')
            ->resetQueryPart('groupBy')
            ->resetQueryPart('orderBy');

        $count = $this->connection->fetchOne($countQuery->toSQL());
        self::assertEquals(5, $count); // 5 users total
    }
}