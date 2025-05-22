<?php

declare(strict_types=1);

namespace Flow\ETL\SQL\QueryBuilder\Tests\Integration;

use Doctrine\DBAL\Connection;
use Doctrine\DBAL\DriverManager;
use PHPUnit\Framework\TestCase;

abstract class DatabaseTestCase extends TestCase
{
    protected ?Connection $connection = null;

    protected function setUp(): void
    {
        parent::setUp();
        $this->connection = $this->createConnection();
        $this->createTestSchema();
    }

    protected function tearDown(): void
    {
        if ($this->connection !== null) {
            $this->dropTestSchema();
            $this->connection->close();
            $this->connection = null;
        }
        parent::tearDown();
    }

    abstract protected function createConnection(): Connection;

    protected function createTestSchema(): void
    {
        // Create users table
        $this->connection->executeStatement('
            CREATE TABLE users (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                email VARCHAR(100),
                status VARCHAR(20),
                created_at TIMESTAMP
            )
        ');

        // Create orders table
        $this->connection->executeStatement('
            CREATE TABLE orders (
                id INTEGER PRIMARY KEY,
                user_id INTEGER NOT NULL,
                amount DECIMAL(10,2) NOT NULL,
                status VARCHAR(20),
                created_at TIMESTAMP
            )
        ');

        // Create categories table for recursive CTE tests
        $this->connection->executeStatement('
            CREATE TABLE categories (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) NOT NULL,
                parent_id INTEGER
            )
        ');

        // Insert test data
        $this->insertTestData();
    }

    protected function dropTestSchema(): void
    {
        $this->connection->executeStatement('DROP TABLE IF EXISTS orders');
        $this->connection->executeStatement('DROP TABLE IF EXISTS users');
        $this->connection->executeStatement('DROP TABLE IF EXISTS categories');
    }

    protected function insertTestData(): void
    {
        // Insert users
        $users = [
            ['id' => 1, 'name' => 'Alice Johnson', 'email' => 'alice@example.com', 'status' => 'active', 'created_at' => '2024-01-01 10:00:00'],
            ['id' => 2, 'name' => 'Bob Smith', 'email' => 'bob@example.com', 'status' => 'active', 'created_at' => '2024-01-15 11:00:00'],
            ['id' => 3, 'name' => 'Charlie Brown', 'email' => 'charlie@example.com', 'status' => 'inactive', 'created_at' => '2024-02-01 12:00:00'],
            ['id' => 4, 'name' => 'Diana Prince', 'email' => 'diana@example.com', 'status' => 'active', 'created_at' => '2024-02-15 13:00:00'],
            ['id' => 5, 'name' => 'Eve Wilson', 'email' => 'eve@example.com', 'status' => 'active', 'created_at' => '2024-03-01 14:00:00'],
        ];

        foreach ($users as $user) {
            $this->connection->insert('users', $user);
        }

        // Insert orders
        $orders = [
            ['id' => 1, 'user_id' => 1, 'amount' => 100.50, 'status' => 'completed', 'created_at' => '2024-01-10 10:00:00'],
            ['id' => 2, 'user_id' => 1, 'amount' => 75.25, 'status' => 'pending', 'created_at' => '2024-01-20 11:00:00'],
            ['id' => 3, 'user_id' => 1, 'amount' => 200.00, 'status' => 'completed', 'created_at' => '2024-02-05 12:00:00'],
            ['id' => 4, 'user_id' => 2, 'amount' => 150.75, 'status' => 'completed', 'created_at' => '2024-01-25 13:00:00'],
            ['id' => 5, 'user_id' => 2, 'amount' => 50.00, 'status' => 'cancelled', 'created_at' => '2024-02-10 14:00:00'],
            ['id' => 6, 'user_id' => 3, 'amount' => 300.00, 'status' => 'completed', 'created_at' => '2024-02-15 15:00:00'],
            ['id' => 7, 'user_id' => 4, 'amount' => 125.50, 'status' => 'completed', 'created_at' => '2024-02-20 16:00:00'],
            ['id' => 8, 'user_id' => 4, 'amount' => 80.00, 'status' => 'pending', 'created_at' => '2024-03-01 17:00:00'],
            ['id' => 9, 'user_id' => 5, 'amount' => 500.00, 'status' => 'completed', 'created_at' => '2024-03-05 18:00:00'],
            ['id' => 10, 'user_id' => 5, 'amount' => 250.00, 'status' => 'completed', 'created_at' => '2024-03-10 19:00:00'],
        ];

        foreach ($orders as $order) {
            $this->connection->insert('orders', $order);
        }

        // Insert categories
        $categories = [
            ['id' => 1, 'name' => 'Electronics', 'parent_id' => null],
            ['id' => 2, 'name' => 'Computers', 'parent_id' => 1],
            ['id' => 3, 'name' => 'Laptops', 'parent_id' => 2],
            ['id' => 4, 'name' => 'Desktops', 'parent_id' => 2],
            ['id' => 5, 'name' => 'Gaming Laptops', 'parent_id' => 3],
            ['id' => 6, 'name' => 'Business Laptops', 'parent_id' => 3],
            ['id' => 7, 'name' => 'Phones', 'parent_id' => 1],
            ['id' => 8, 'name' => 'Smartphones', 'parent_id' => 7],
            ['id' => 9, 'name' => 'Feature Phones', 'parent_id' => 7],
            ['id' => 10, 'name' => 'Clothing', 'parent_id' => null],
            ['id' => 11, 'name' => 'Men', 'parent_id' => 10],
            ['id' => 12, 'name' => 'Women', 'parent_id' => 10],
            ['id' => 13, 'name' => 'T-Shirts', 'parent_id' => 11],
            ['id' => 14, 'name' => 'Jeans', 'parent_id' => 11],
        ];

        foreach ($categories as $category) {
            $this->connection->insert('categories', $category);
        }
    }

    protected function isPlatformSupported(string $feature): bool
    {
        $platform = $this->connection->getDatabasePlatform();
        $platformName = strtolower($platform->getName());

        return match ($feature) {
            'cte' => in_array($platformName, ['postgresql', 'mysql', 'sqlite']),
            'recursive_cte' => in_array($platformName, ['postgresql', 'mysql', 'sqlite']),
            'lateral_join' => in_array($platformName, ['postgresql', 'mysql']),
            default => true,
        };
    }
}