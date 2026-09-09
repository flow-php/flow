<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{from_dbal_queries,
    sqlite_insert_options,
    to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, to_output};
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
    // second insert that normally would fail due to Integrity constraint violation
    ->write(
        to_dbal_table_insert(
            $connection,
            'orders',
            sqlite_insert_options(conflict_columns: ['order_id'])
        )
    )
    ->run();

data_frame()
    ->read(
        from_dbal_queries(
            $connection,
            'SELECT COUNT(*) as total_rows FROM orders'
        )
    )
    ->write(to_output(truncate: false))
    ->run();
