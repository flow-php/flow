<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\Adapter\Doctrine\{from_dbal_key_set_qb,
    pagination_key_asc,
    pagination_key_set,
    to_dbal_table_insert};
use function Flow\ETL\DSL\{data_frame, to_output};
use Doctrine\DBAL\DriverManager;
use Doctrine\DBAL\Schema\{Column, Table};
use Doctrine\DBAL\Types\{Type, Types};

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
]));

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(20)
    ->write(to_dbal_table_insert($connection, 'orders'))
    ->run();

// LIMIT/OFFSET makes the database skip every row of every earlier page. Keyset remembers the last
// row instead and asks for what follows it, so page 500 costs the same as page 1.
data_frame()
    ->read(
        from_dbal_key_set_qb(
            $connection,
            $connection->createQueryBuilder()
                ->select('order_id', 'created_at', 'customer')
                ->from('orders')
                ->setMaxResults(5),
            pagination_key_set(pagination_key_asc('order_id')),
        )
    )
    ->limit(10)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
