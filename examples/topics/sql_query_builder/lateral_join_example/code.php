<?php

declare(strict_types=1);

use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

require __DIR__ . '/vendor/autoload.php';

// Note: This example requires PostgreSQL as SQLite doesn't support LATERAL
$connection = DriverManager::getConnection([
    'driver' => 'pdo_pgsql',
    'host' => 'localhost',
    'dbname' => 'flow_example',
    'user' => 'postgres',
    'password' => 'postgres',
]);

// Create tables for example
$connection->executeStatement('DROP TABLE IF EXISTS orders CASCADE');
$connection->executeStatement('DROP TABLE IF EXISTS users CASCADE');

$connection->executeStatement('
    CREATE TABLE users (
        id SERIAL PRIMARY KEY,
        name VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
');

$connection->executeStatement('
    CREATE TABLE orders (
        id SERIAL PRIMARY KEY,
        user_id INTEGER REFERENCES users(id),
        amount DECIMAL(10,2),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    )
');

// Insert sample data
$connection->executeStatement("
    INSERT INTO users (name, created_at) VALUES
    ('Alice', '2024-01-01'),
    ('Bob', '2024-01-15'),
    ('Charlie', '2024-02-01')
");

$connection->executeStatement("
    INSERT INTO orders (user_id, amount, status, created_at) VALUES
    (1, 100.00, 'completed', '2024-01-10'),
    (1, 150.00, 'completed', '2024-01-20'),
    (1, 200.00, 'pending', '2024-02-05'),
    (1, 75.00, 'completed', '2024-02-15'),
    (2, 300.00, 'completed', '2024-01-20'),
    (2, 250.00, 'cancelled', '2024-02-01'),
    (2, 180.00, 'completed', '2024-02-10'),
    (3, 500.00, 'completed', '2024-02-05'),
    (3, 120.00, 'pending', '2024-02-20')
");

// Create Flow QueryBuilder with PostgreSQL platform
$platform = PlatformFactory::fromConnection($connection);
$builder = new FlowQueryBuilder($platform);

// Build lateral subquery for recent orders
$recentOrdersBuilder = new FlowQueryBuilder($platform);
$recentOrders = $recentOrdersBuilder
    ->select('o.id', 'o.amount', 'o.status', 'o.created_at')
    ->from('orders', 'o')
    ->where('o.user_id = u.id')
    ->andWhere('o.status = :status')
    ->orderBy('o.created_at', 'DESC')
    ->limit(3)
    ->setParameter('status', 'completed');

// Build main query with LATERAL JOIN
$query = $builder
    ->select(
        'u.id as user_id',
        'u.name',
        'recent_orders.id as order_id',
        'recent_orders.amount',
        'recent_orders.created_at as order_date'
    )
    ->from('users', 'u')
    ->leftLateralJoin('u', $recentOrders, 'recent_orders')
    ->orderBy('u.id')
    ->addOrderBy('recent_orders.created_at', 'DESC');

// Execute query
$result = $connection->executeQuery(
    $query->toSQL(),
    $query->getParameters(),
    $query->getParameterTypes()
);

// Display results
echo "Users with Their Recent Completed Orders (Max 3 per user):\n";
echo "==========================================================\n";
printf("%-10s %-15s %-10s %-10s %-20s\n", "User ID", "Name", "Order ID", "Amount", "Order Date");
echo "----------------------------------------------------------\n";

foreach ($result->fetchAllAssociative() as $row) {
    printf(
        "%-10d %-15s %-10s $%-9.2f %-20s\n",
        $row['user_id'],
        $row['name'],
        $row['order_id'] ?? 'N/A',
        $row['amount'] ?? 0,
        $row['order_date'] ?? 'N/A'
    );
}

// Show the generated SQL
echo "\nGenerated SQL:\n";
echo "==============\n";
echo $query->toSQL() . "\n";

// Show query parts for debugging
echo "\nQuery Parts:\n";
echo "============\n";
print_r($query->getQueryParts());