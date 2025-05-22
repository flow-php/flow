<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

class PostgreSQLIntegrationTest extends DatabaseTestCase
{
    protected function createConnection(): Connection
    {
        if (!extension_loaded('pdo_pgsql')) {
            $this->markTestSkipped('PDO PostgreSQL extension is not available');
        }

        $host = $_ENV['POSTGRES_HOST'] ?? 'localhost';
        $port = $_ENV['POSTGRES_PORT'] ?? '5432';
        $user = $_ENV['POSTGRES_USER'] ?? 'postgres';
        $password = $_ENV['POSTGRES_PASSWORD'] ?? 'postgres';
        $dbname = $_ENV['POSTGRES_DB'] ?? 'flow_test';

        try {
            return DriverManager::getConnection([
                'driver' => 'pdo_pgsql',
                'host' => $host,
                'port' => $port,
                'user' => $user,
                'password' => $password,
                'dbname' => $dbname,
            ]);
        } catch (\Exception $e) {
            $this->markTestSkipped('PostgreSQL connection failed: ' . $e->getMessage());
        }
    }

    protected function createTestSchema(): void
    {
        // Drop tables if they exist
        $this->connection->executeStatement('DROP TABLE IF EXISTS orders CASCADE');
        $this->connection->executeStatement('DROP TABLE IF EXISTS users CASCADE');
        $this->connection->executeStatement('DROP TABLE IF EXISTS categories CASCADE');

        // PostgreSQL specific schema
        $this->connection->executeStatement('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                status VARCHAR(20),
                created_at TIMESTAMP
            )
        ');

        $this->connection->executeStatement('
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL REFERENCES users(id),
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20),
                created_at TIMESTAMP
            )
        ');

        $this->connection->executeStatement('
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INTEGER REFERENCES categories(id)
            )
        ');

        $this->insertTestData();
    }

    public function test_lateral_join(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // Subquery to get top 2 orders per user
        $topOrdersBuilder = new FlowQueryBuilder($platform);
        $topOrders = $topOrdersBuilder
            ->select('o.id', 'o.amount', 'o.status')
            ->from('orders', 'o')
            ->where('o.user_id = u.id')
            ->andWhere('o.status = :status')
            ->orderBy('o.amount', 'DESC')
            ->limit(2)
            ->setParameter('status', 'completed');

        $sql = $builder
            ->select('u.name', 'top_orders.amount', 'top_orders.status')
            ->from('users', 'u')
            ->leftLateralJoin('u', $topOrders, 'top_orders')
            ->where('u.status = :user_status')
            ->orderBy('u.name')
            ->addOrderBy('top_orders.amount', 'DESC')
            ->setParameter('user_status', 'active');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // Each active user should have at most 2 completed orders
        $userOrders = [];
        foreach ($result as $row) {
            $userOrders[$row['name']][] = $row['amount'];
        }

        foreach ($userOrders as $user => $orders) {
            self::assertLessThanOrEqual(2, count($orders));
            // Orders should be sorted by amount DESC
            for ($i = 1; $i < count($orders); $i++) {
                self::assertGreaterThanOrEqual($orders[$i], $orders[$i - 1]);
            }
        }
    }

    public function test_window_functions_with_cte(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // CTE to calculate order statistics
        $orderStatsBuilder = new FlowQueryBuilder($platform);
        $orderStats = $orderStatsBuilder
            ->select(
                'user_id',
                'COUNT(*) as order_count',
                'SUM(amount) as total_amount',
                'AVG(amount) as avg_amount'
            )
            ->from('orders')
            ->where('status = :status')
            ->groupBy('user_id')
            ->setParameter('status', 'completed');

        $sql = $builder
            ->with('order_stats', $orderStats)
            ->select(
                'u.name',
                'os.order_count',
                'os.total_amount',
                'os.avg_amount',
                'RANK() OVER (ORDER BY os.total_amount DESC) as revenue_rank'
            )
            ->from('users', 'u')
            ->join('u', 'order_stats', 'os', 'os.user_id = u.id')
            ->orderBy('revenue_rank');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertGreaterThan(0, count($result));
        
        // Verify ranking is correct
        $previousRank = 0;
        $previousAmount = PHP_FLOAT_MAX;
        foreach ($result as $row) {
            self::assertGreaterThanOrEqual($previousRank, $row['revenue_rank']);
            if ($row['revenue_rank'] > $previousRank) {
                self::assertLessThan($previousAmount, $row['total_amount']);
            }
            $previousRank = $row['revenue_rank'];
            $previousAmount = $row['total_amount'];
        }
    }

    public function test_recursive_cte_with_path(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // Build category path using recursive CTE
        $initial = "SELECT id, name, parent_id, name::text as path 
                   FROM categories 
                   WHERE parent_id IS NULL";
        
        $recursive = "SELECT c.id, c.name, c.parent_id, 
                     (ct.path || ' > ' || c.name)::text as path 
                     FROM categories c 
                     JOIN category_tree ct ON c.parent_id = ct.id";

        $sql = $builder
            ->withRecursive('category_tree', $initial, $recursive)
            ->select('id', 'name', 'path')
            ->from('category_tree')
            ->orderBy('path');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // Verify paths are built correctly
        foreach ($result as $row) {
            self::assertNotEmpty($row['path']);
            if (str_contains($row['path'], '>')) {
                // Child categories should have parent in path
                $parts = explode(' > ', $row['path']);
                self::assertGreaterThan(1, count($parts));
                self::assertEquals($row['name'], end($parts));
            }
        }
    }

    public function test_multiple_ctes(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // First CTE: Active users
        $activeUsersBuilder = new FlowQueryBuilder($platform);
        $activeUsers = $activeUsersBuilder
            ->select('id', 'name')
            ->from('users')
            ->where('status = :status')
            ->setParameter('status', 'active');

        // Second CTE: Completed orders
        $completedOrdersBuilder = new FlowQueryBuilder($platform);
        $completedOrders = $completedOrdersBuilder
            ->select('user_id', 'SUM(amount) as total')
            ->from('orders')
            ->where('status = :order_status')
            ->groupBy('user_id')
            ->setParameter('order_status', 'completed');

        $sql = $builder
            ->with('active_users', $activeUsers)
            ->with('completed_orders', $completedOrders)
            ->select('au.name', 'COALESCE(co.total, 0) as total_revenue')
            ->from('active_users', 'au')
            ->leftJoin('au', 'completed_orders', 'co', 'co.user_id = au.id')
            ->orderBy('total_revenue', 'DESC');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(4, $result); // 4 active users
        foreach ($result as $row) {
            self::assertArrayHasKey('name', $row);
            self::assertArrayHasKey('total_revenue', $row);
            self::assertGreaterThanOrEqual(0, $row['total_revenue']);
        }
    }

    public function test_complex_lateral_join_with_cte(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // CTE for user order summaries
        $userSummaryBuilder = new FlowQueryBuilder($platform);
        $userSummary = $userSummaryBuilder
            ->select(
                'u.id',
                'u.name',
                'COUNT(o.id) as total_orders'
            )
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->groupBy('u.id', 'u.name');

        // Lateral subquery for recent orders
        $recentOrdersBuilder = new FlowQueryBuilder($platform);
        $recentOrders = $recentOrdersBuilder
            ->select('amount', 'created_at')
            ->from('orders')
            ->where('user_id = us.id')
            ->orderBy('created_at', 'DESC')
            ->limit(3);

        $sql = $builder
            ->with('user_summary', $userSummary)
            ->select(
                'us.name',
                'us.total_orders',
                'recent.amount as recent_amount',
                'recent.created_at as recent_date'
            )
            ->from('user_summary', 'us')
            ->leftLateralJoin('us', $recentOrders, 'recent')
            ->where('us.total_orders > :min_orders')
            ->orderBy('us.name')
            ->addOrderBy('recent.created_at', 'DESC')
            ->setParameter('min_orders', 0);

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // Verify results
        $currentUser = null;
        $orderCount = 0;
        foreach ($result as $row) {
            if ($currentUser !== $row['name']) {
                $currentUser = $row['name'];
                $orderCount = 0;
            }
            $orderCount++;
            self::assertLessThanOrEqual(3, $orderCount); // Max 3 recent orders per user
        }
    }
}