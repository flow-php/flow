<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_key_set,
    pgsql_pagination_key_asc,
    pgsql_pagination_key_set,
    to_pgsql_table};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};
use function Flow\PostgreSql\DSL\{asc, col, column, column_type_text, column_type_timestamp, column_type_uuid, create, drop, pgsql_client, pgsql_connection_dsn, select, table};

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

data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'created_at', 'customer')
    ->limit(20)
    ->write(to_pgsql_table($client, 'orders'))
    ->run();

// each page carries the previous page's last key into a WHERE, so PostgreSQL seeks the index
// instead of counting past every skipped row - the cost of page 5000 equals the cost of page 1
data_frame()
    ->read(
        from_pgsql_key_set(
            $client,
            select(col('order_id'), col('created_at'), col('customer'))
                ->from(table('orders'))
                ->orderBy(asc(col('order_id'))),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('order_id')),
        )
    )
    ->limit(5)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
