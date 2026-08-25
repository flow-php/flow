<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{from_dbal_limit_offset, to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, to_output};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};
use Flow\ETL\Adapter\Doctrine\{Order, OrderBy};

require __DIR__ . '/vendor/autoload.php';

if (!\extension_loaded('pdo_sqlite')) {
    print 'Example skipped. Requires PDO SQLite extension which is not available in this environment.' . PHP_EOL;

    return;
}

$connection = DriverManager::getConnection([
    'path' => __DIR__ . '/output/orders.db',
    'driver' => 'pdo_sqlite',
]);

$schemaManager = $connection->createSchemaManager();

if ($schemaManager->tablesExist(['orders'])) {
    $schemaManager->dropTable('orders');
}

$schemaManager->createTable(new Table(
    'orders',
    [
        new Column('order_id', Type::getType(Types::GUID), ['notnull' => true]),
        new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
        new Column('email', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        new Column('customer', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
    ],
));

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'email', 'customer')
    ->limit(10)
    ->write(to_dbal_table_insert($connection, 'orders'))
    ->run();

data_frame()
    ->read(
        from_dbal_limit_offset(
            $connection,
            'orders',
            new OrderBy('created_at', Order::DESC),
        )
    )
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
