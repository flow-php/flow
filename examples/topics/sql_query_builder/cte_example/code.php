<?php

declare(strict_types=1);

use Doctrine\DBAL\DriverManager;
use Flow\ETL\SQL\QueryBuilder\FlowQueryBuilder;
use Flow\ETL\SQL\QueryBuilder\Platform\PlatformFactory;

require __DIR__ . '/vendor/autoload.php';

// Create connection
$connection = DriverManager::getConnection([
    'driver' => 'pdo_sqlite',
    'memory' => true,
]);

// Create tables for example
$connection->executeStatement('
    CREATE TABLE sales (
        id INTEGER PRIMARY KEY,
        product_id INTEGER,
        amount DECIMAL(10,2),
        sale_date DATE
    )
');

// Insert sample data
$connection->executeStatement("
    INSERT INTO sales (product_id, amount, sale_date) VALUES
    (1, 100.00, '2024-01-15'),
    (2, 150.00, '2024-01-20'),
    (1, 200.00, '2024-02-10'),
    (3, 300.00, '2024-02-15'),
    (2, 250.00, '2024-03-05'),
    (1, 180.00, '2024-03-20')
");

// Create Flow QueryBuilder
$platform = PlatformFactory::fromConnection($connection);
$builder = new FlowQueryBuilder($platform);

// Build monthly sales CTE
$monthlySalesBuilder = new FlowQueryBuilder($platform);
$monthlySales = $monthlySalesBuilder
    ->select(
        "strftime('%Y-%m', sale_date) as month",
        'product_id',
        'SUM(amount) as total_sales'
    )
    ->from('sales')
    ->groupBy('month', 'product_id');

// Build main query with CTE
$query = $builder
    ->with('monthly_product_sales', $monthlySales)
    ->select(
        'month',
        'product_id',
        'total_sales',
        'SUM(total_sales) OVER (PARTITION BY product_id ORDER BY month) as running_total'
    )
    ->from('monthly_product_sales')
    ->orderBy('product_id')
    ->addOrderBy('month');

// Execute query
$result = $connection->executeQuery(
    $query->toSQL(),
    $query->getParameters(),
    $query->getParameterTypes()
);

// Display results
echo "Monthly Product Sales with Running Totals:\n";
echo "==========================================\n";
printf("%-10s %-12s %-12s %-15s\n", "Month", "Product ID", "Sales", "Running Total");
echo "------------------------------------------\n";

foreach ($result->fetchAllAssociative() as $row) {
    printf(
        "%-10s %-12d $%-11.2f $%-14.2f\n",
        $row['month'],
        $row['product_id'],
        $row['total_sales'],
        $row['running_total']
    );
}

// Show the generated SQL
echo "\nGenerated SQL:\n";
echo "==============\n";
echo $query->toSQL() . "\n";