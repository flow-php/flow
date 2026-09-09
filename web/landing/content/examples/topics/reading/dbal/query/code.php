<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{from_dbal_queries, to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, to_output};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\ParametersSet;

require __DIR__ . '/vendor/autoload.php';

if (!\extension_loaded('pdo_sqlite')) {
    print 'Example skipped. Requires PDO SQLite extension which is not available in this environment.' . PHP_EOL;

    return;
}

$connection = DriverManager::getConnection(['memory' => true, 'driver' => 'pdo_sqlite']);
$connection->createSchemaManager()->createTable(new Table('orders', [
    new Column('order_id', Type::getType(Types::GUID), ['notnull' => true]),
    new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
    new Column('customer', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
    new Column('discount', Type::getType(Types::FLOAT), ['notnull' => false]),
]));

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer', 'discount')
    ->limit(50)
    ->write(to_dbal_table_insert($connection, 'orders'))
    ->run();

// the whole query is yours - joins, aggregates, window functions - and the database does the work
// instead of shipping every row to PHP for Flow to reduce
data_frame()
    ->read(
        from_dbal_queries(
            $connection,
            "SELECT strftime('%Y-%m', created_at) AS month,
                    COUNT(*) AS orders,
                    ROUND(AVG(discount), 2) AS avg_discount
             FROM orders
             GROUP BY month
             ORDER BY orders DESC
             LIMIT :limit",
            new ParametersSet(['limit' => 5]),
        )
    )
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
