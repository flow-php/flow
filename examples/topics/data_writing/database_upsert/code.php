<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\Doctrine\{from_dbal_query, to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, from_array, to_stream};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table, UniqueConstraint};
use Doctrine\DBAL\Types\{Type, Types};

require __DIR__ . '/../../../autoload.php';

require __DIR__ . '/generate_static_orders.php';

$connection = DriverManager::getConnection([
    'path' => __DIR__ . '/output/orders.db',
    'driver' => 'pdo_sqlite',
]);

$schemaManager = $connection->createSchemaManager();

if (!$schemaManager->tablesExist(['orders'])) {
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
        uniqueConstraints: [
            new UniqueConstraint('orders_order_id', ['order_id']),
        ]
    ));
}

$orderIds = [
    'c0a43894-0102-4a4e-9fcd-393ef9e4f16a',
    '83fd51a4-9bd1-4b40-8f6e-6a7cc940bb5a',
    '7c65db1a-410f-4e91-8aeb-66fb3f1665f7',
    '5af1d56c-a9f7-411e-8738-865942d6c40f',
    '3a3ae1a9-debd-425a-8f9d-63c3315bc483',
    '27d8ee4d-94cc-47fa-bc14-209a4ab2eb45',
    'cc4fd722-1407-4781-9ad4-fa53966060af',
    '718360e1-c4c9-40f4-84e2-6f7898788883',
    'ea7c731c-ce3b-40bb-bbf8-79f1c717b6ca',
    '17b0d6c5-dd8f-4d5a-ae06-1df15b67c82c',
];

data_frame()
    ->read(from_array(generateStaticOrders($orderIds)))
    ->write(
        to_dbal_table_insert(
            DriverManager::getConnection([
                'path' => __DIR__ . '/output/orders.db',
                'driver' => 'pdo_sqlite',
            ]),
            'orders',
            [
                'conflict_columns' => ['order_id'],
            ]
        )
    )
    // second insert that normally would fail due to Integrity constraint violation
    ->write(
        to_dbal_table_insert(
            DriverManager::getConnection([
                'path' => __DIR__ . '/output/orders.db',
                'driver' => 'pdo_sqlite',
            ]),
            'orders',
            [
                'conflict_columns' => ['order_id'],
            ]
        )
    )
    ->run();

data_frame()
    ->read(
        from_dbal_query(
            DriverManager::getConnection([
                'path' => __DIR__ . '/output/orders.db',
                'driver' => 'pdo_sqlite',
            ]),
            'SELECT COUNT(*) as total_rows FROM orders'
        )
    )
    ->write(to_stream(__DIR__ . '/output.txt', truncate: false))
    ->run();
