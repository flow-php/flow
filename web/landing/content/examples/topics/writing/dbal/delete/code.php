<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{
    sqlite_insert_options,
    to_dbal_table_delete,
    to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, from_array};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table, UniqueConstraint};
use Doctrine\DBAL\Types\{Type, Types};

require __DIR__ . '/vendor/autoload.php';

if (!\extension_loaded('pdo_sqlite')) {
    print 'Example skipped. Requires PDO SQLite extension which is not available in this environment.' . PHP_EOL;

    return;
}

$connection = DriverManager::getConnection([
    'memory' => true,
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

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'updated_at', 'discount', 'email', 'customer', 'address', 'notes', 'items')
    ->limit(10)
    ->write(
        to_dbal_table_insert(
            $connection,
            'orders',
            sqlite_insert_options(conflict_columns: ['order_id'])
        )
    )
    ->run();

$orderIds = \array_column(
    $connection->fetchAllAssociative('SELECT order_id FROM orders'),
    'order_id'
);

data_frame()
    ->read(from_array(\array_map(static fn (string $id) => ['order_id' => $id], $orderIds)))
    ->write(
        to_dbal_table_delete(
            $connection,
            'orders',
        )
    )
    ->run();

// the delete is the lesson, so read the table back and show what is left
echo 'orders after delete: ' . \count($connection->fetchAllAssociative('SELECT * FROM orders')) . PHP_EOL;
