<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_cursor, pgsql_insert_options, to_pgsql_table};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};
use function Flow\PostgreSql\DSL\{column, column_type_text, column_type_timestamp, column_type_uuid, count_all, create, drop, pgsql_client, pgsql_connection_dsn, select, table};

require __DIR__ . '/vendor/autoload.php';

if (!\extension_loaded('pgsql')) {
    print 'Example skipped. Requires the pgsql extension and a running PostgreSQL server.' . PHP_EOL;

    return;
}

$client = pgsql_client(pgsql_connection_dsn('postgres://postgres:postgres@127.0.0.1:5452/postgres'));

$client->execute(drop()->table('orders')->ifExists());
$client->execute(
    create()->table('orders')
        ->column(column('order_id', column_type_uuid())->primaryKey())
        ->column(column('created_at', column_type_timestamp())->notNull())
        ->column(column('customer', column_type_text())->notNull())
);

$orders = data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(10);

// ON CONFLICT (order_id) DO UPDATE - the second write would violate the primary key, instead it
// overwrites the columns you name
$upsert = to_pgsql_table($client, 'orders')
    ->withInsertOptions(pgsql_insert_options(conflictColumns: ['order_id'], updateColumns: ['customer']));

$orders->write($upsert)->run();
$orders->write($upsert)->run();

data_frame()
    ->read(from_pgsql_cursor($client, select(count_all()->as('total'))->from(table('orders'))))
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
