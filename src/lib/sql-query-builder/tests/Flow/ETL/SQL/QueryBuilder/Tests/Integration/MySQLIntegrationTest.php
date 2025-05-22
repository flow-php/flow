<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

class MySQLIntegrationTest extends DatabaseTestCase
{
    protected function createConnection(): Connection
    {
        if (!extension_loaded('pdo_mysql')) {
            $this->markTestSkipped('PDO MySQL extension is not available');
        }

        $host = $_ENV['MYSQL_HOST'] ?? 'localhost';
        $port = $_ENV['MYSQL_PORT'] ?? '3306';
        $user = $_ENV['MYSQL_USER'] ?? 'root';
        $password = $_ENV['MYSQL_PASSWORD'] ?? 'root';
        $dbname = $_ENV['MYSQL_DATABASE'] ?? 'flow_test';

        try {
            return DriverManager::getConnection([
                'driver' => 'pdo_mysql',
                'host' => $host,
                'port' => $port,
                'user' => $user,
                'password' => $password,
                'dbname' => $dbname,
                'charset' => 'utf8mb4',
            ]);
        } catch (\Exception $e) {
            $this->markTestSkipped('MySQL connection failed: ' . $e->getMessage());
        }
    }

    protected function createTestSchema(): void
    {
        // Disable foreign key checks for dropping tables
        $this->connection->executeStatement('SET FOREIGN_KEY_CHECKS = 0');
        
        // Drop tables if they exist
        $this->connection->executeStatement('DROP TABLE IF EXISTS orders');
        $this->connection->executeStatement('DROP TABLE IF EXISTS users');
        $this->connection->executeStatement('DROP TABLE IF EXISTS categories');
        
        // Re-enable foreign key checks
        $this->connection->executeStatement('SET FOREIGN_KEY_CHECKS = 1');

        // MySQL specific schema
        $this->connection->executeStatement('
            CREATE TABLE users (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                status VARCHAR(20),
                created_at TIMESTAMP NULL
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ');

        $this->connection->executeStatement('
            CREATE TABLE orders (
                id INT PRIMARY KEY,
                user_id INT NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20),
                created_at TIMESTAMP NULL,
                FOREIGN KEY (user_id) REFERENCES users(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ');

        $this->connection->executeStatement('
            CREATE TABLE categories (
                id INT PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INT,
                FOREIGN KEY (parent_id) REFERENCES categories(id)
            ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4
        ');

        $this->insertTestData();
    }

    protected function dropTestSchema(): void
    {
        $this->connection->executeStatement('SET FOREIGN_KEY_CHECKS = 0');
        parent::dropTestSchema();
        $this->connection->executeStatement('SET FOREIGN_KEY_CHECKS = 1');
    }

    public function test_mysql_specific_functions(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $sql = $builder
            ->select(
                'u.name',
                'COUNT(o.id) as order_count',
                'GROUP_CONCAT(o.status ORDER BY o.created_at SEPARATOR ", ") as order_statuses'
            )
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->groupBy('u.id', 'u.name')
            ->having('order_count > 0')
            ->orderBy('u.name');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        foreach ($result as $row) {
            self::assertNotEmpty($row['order_statuses']);
            $statuses = explode(', ', $row['order_statuses']);
            self::assertEquals($row['order_count'], count($statuses));
        }
    }

    public function test_cte_support_mysql8(): void
    {
        // Check MySQL version
        $version = $this->connection->fetchOne('SELECT VERSION()');
        if (version_compare($version, '8.0', '<')) {
            $this->markTestSkipped('MySQL 8.0+ required for CTE support');
        }

        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // Monthly sales CTE
        $monthlySalesBuilder = new FlowQueryBuilder($platform);
        $monthlySales = $monthlySalesBuilder
            ->select(
                'DATE_FORMAT(created_at, "%Y-%m") as month',
                'SUM(amount) as total_sales'
            )
            ->from('orders')
            ->where('status = :status')
            ->groupBy('month')
            ->setParameter('status', 'completed');

        $sql = $builder
            ->with('monthly_sales', $monthlySales)
            ->select('*')
            ->from('monthly_sales')
            ->orderBy('month');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertGreaterThan(0, count($result));
        foreach ($result as $row) {
            self::assertArrayHasKey('month', $row);
            self::assertArrayHasKey('total_sales', $row);
        }
    }

    public function test_lateral_join_mysql8(): void
    {
        // Check MySQL version
        $version = $this->connection->fetchOne('SELECT VERSION()');
        if (version_compare($version, '8.0.14', '<')) {
            $this->markTestSkipped('MySQL 8.0.14+ required for LATERAL support');
        }

        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        // Latest orders per user
        $latestOrdersBuilder = new FlowQueryBuilder($platform);
        $latestOrders = $latestOrdersBuilder
            ->select('id', 'amount', 'status', 'created_at')
            ->from('orders')
            ->where('user_id = u.id')
            ->orderBy('created_at', 'DESC')
            ->limit(2);

        $sql = $builder
            ->select('u.name', 'latest.amount', 'latest.status')
            ->from('users', 'u')
            ->leftLateralJoin('u', $latestOrders, 'latest')
            ->where('u.status = :status')
            ->orderBy('u.name')
            ->setParameter('status', 'active');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // Verify each user has at most 2 orders
        $userOrderCounts = [];
        foreach ($result as $row) {
            $userOrderCounts[$row['name']] = ($userOrderCounts[$row['name']] ?? 0) + 1;
        }

        foreach ($userOrderCounts as $user => $count) {
            self::assertLessThanOrEqual(2, $count);
        }
    }

    public function test_recursive_cte_mysql8(): void
    {
        // Check MySQL version
        $version = $this->connection->fetchOne('SELECT VERSION()');
        if (version_compare($version, '8.0', '<')) {
            $this->markTestSkipped('MySQL 8.0+ required for recursive CTE support');
        }

        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $initial = "SELECT id, name, parent_id, 0 as depth 
                   FROM categories 
                   WHERE parent_id IS NULL";
        
        $recursive = "SELECT c.id, c.name, c.parent_id, ct.depth + 1 
                     FROM categories c 
                     JOIN category_hierarchy ct ON c.parent_id = ct.id 
                     WHERE ct.depth < 10"; // Prevent infinite recursion

        $sql = $builder
            ->withRecursive('category_hierarchy', $initial, $recursive)
            ->select('*')
            ->from('category_hierarchy')
            ->orderBy('depth')
            ->addOrderBy('name');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // Verify hierarchy depth
        $maxDepth = 0;
        foreach ($result as $row) {
            self::assertArrayHasKey('depth', $row);
            self::assertIsNumeric($row['depth']);
            $maxDepth = max($maxDepth, (int)$row['depth']);
        }
        
        self::assertGreaterThan(0, $maxDepth); // Should have nested categories
    }

    public function test_mysql_limit_offset_syntax(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $sql = $builder
            ->select('*')
            ->from('users')
            ->orderBy('created_at')
            ->limit(3)
            ->offset(2);

        // MySQL uses LIMIT offset, count syntax
        $sqlString = $sql->toSQL();
        self::assertStringContainsString('LIMIT 2, 3', $sqlString);

        $result = $this->connection->executeQuery(
            $sqlString,
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        self::assertCount(3, $result);
        self::assertEquals('Charlie Brown', $result[0]['name']); // Offset 2
    }

    public function test_json_operations(): void
    {
        // Add a JSON column for testing
        try {
            $this->connection->executeStatement('ALTER TABLE users ADD COLUMN metadata JSON');
            
            // Update users with JSON data
            $this->connection->executeStatement("
                UPDATE users 
                SET metadata = JSON_OBJECT('age', id * 10, 'premium', IF(status = 'active', true, false))
            ");
        } catch (\Exception $e) {
            $this->markTestSkipped('JSON column creation failed: ' . $e->getMessage());
        }

        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $sql = $builder
            ->select(
                'name',
                'JSON_EXTRACT(metadata, "$.age") as age',
                'JSON_EXTRACT(metadata, "$.premium") as is_premium'
            )
            ->from('users')
            ->where('JSON_EXTRACT(metadata, "$.premium") = true')
            ->orderBy('name');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        foreach ($result as $row) {
            self::assertNotNull($row['age']);
            self::assertEquals(1, $row['is_premium']); // MySQL returns 1 for true
        }
    }

    public function test_multiple_aggregations_with_rollup(): void
    {
        $platform = PlatformFactory::fromConnection($this->connection);
        $builder = new FlowQueryBuilder($platform);

        $sql = $builder
            ->select(
                'u.status',
                'o.status as order_status',
                'COUNT(*) as count',
                'SUM(o.amount) as total_amount'
            )
            ->from('users', 'u')
            ->leftJoin('u', 'orders', 'o', 'o.user_id = u.id')
            ->groupBy('u.status, o.status WITH ROLLUP')
            ->orderBy('u.status')
            ->addOrderBy('o.status');

        $result = $this->connection->executeQuery(
            $sql->toSQL(),
            $sql->getParameters(),
            $sql->getParameterTypes()
        )->fetchAllAssociative();

        // WITH ROLLUP should create summary rows
        $hasNullStatus = false;
        foreach ($result as $row) {
            if ($row['status'] === null || $row['order_status'] === null) {
                $hasNullStatus = true;
                break;
            }
        }
        self::assertTrue($hasNullStatus, 'ROLLUP should create rows with NULL values');
    }
}