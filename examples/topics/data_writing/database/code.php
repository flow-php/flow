<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\to_dbal_table_insert;
use function Flow\ETL\DSL\{data_frame, from_array, overwrite};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};

require __DIR__ . '/../../../autoload.php';

$generateOrders = require __DIR__ . '/generate_orders.php';

$connection = DriverManager::getConnection([
    'path' => __DIR__ . '/output/orders.db',
    'driver' => 'pdo_sqlite',
]);

$schemaManager = $connection->createSchemaManager();

if ($schemaManager->tablesExist(['orders'])) {
    $schemaManager->dropTable('orders');
}

$schemaManager->createTable(new Table(
    $table = 'orders',
    [
        new Column('order_id', Type::getType(Types::GUID), ['notnull' => true]),
        new Column('created_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => true]),
        new Column('updated_at', Type::getType(Types::DATETIME_IMMUTABLE), ['notnull' => false]),
        new Column('discount', Type::getType(Types::FLOAT), ['notnull' => false]),
        new Column('email', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        new Column('customer', Type::getType(Types::STRING), ['notnull' => true, 'length' => 255]),
        new Column('address', Type::getType(Types::JSON), ['notnull' => true]),
        new Column('notes', Type::getType(Types::JSON), ['notnull' => true]),
        new Column('items', Type::getType(Types::JSON), ['notnull' => true]),
    ],
));

data_frame()
    ->read(from_array(generateOrders(10)))
    ->saveMode(overwrite())
    ->write(
        to_dbal_table_insert(
            DriverManager::getConnection([
                'path' => __DIR__ . '/output/orders.db',
                'driver' => 'pdo_sqlite',
            ]),
            'orders',
        )
    )
    ->run();
