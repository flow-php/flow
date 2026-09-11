<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_cursor, to_pgsql_table};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};
use function Flow\PostgreSql\DSL\{col, column, column_type_text, column_type_timestamp, column_type_uuid, create, drop, gt, param, pgsql_client, pgsql_connection_dsn, select, table};

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

// a server-side cursor: the result set stays in PostgreSQL and rows are fetched in batches, so a
// billion-row query costs the same memory as a small one. No ORDER BY requirement, one snapshot.
// $1 is bound by position - this extractor uses native placeholders, not named ones.
data_frame()
    ->read(
        from_pgsql_cursor(
            $client,
            select(col('order_id'), col('created_at'), col('customer'))
                ->from(table('orders'))
                ->where(gt(col('created_at'), param(1))),
            ['2025-06-01'],
        )
    )
    ->limit(5)
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
