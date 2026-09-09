<?php

declare(strict_types=1);

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_cursor, to_pgsql_table, to_pgsql_transaction};
use function Flow\ETL\Adapter\CSV\from_csv;
use function Flow\ETL\DSL\{data_frame, to_output};
use function Flow\PostgreSql\DSL\{column, column_type_text, column_type_uuid, count_all, create, drop, literal, pgsql_client, pgsql_connection_dsn, select, table};

require __DIR__ . '/vendor/autoload.php';

if (!\extension_loaded('pgsql')) {
    print 'Example skipped. Requires the pgsql extension and a running PostgreSQL server.' . PHP_EOL;

    return;
}

$client = pgsql_client(pgsql_connection_dsn('postgres://postgres:postgres@127.0.0.1:5452/postgres'));

$client->execute(drop()->table('orders', 'orders_audit')->ifExists());
$client->execute(
    create()->table('orders')
        ->column(column('order_id', column_type_uuid())->primaryKey())
        ->column(column('customer', column_type_text())->notNull())
);
$client->execute(
    create()->table('orders_audit')
        ->column(column('order_id', column_type_uuid())->notNull())
        ->column(column('customer', column_type_text())->notNull())
);

// both loaders share one Client, so both tables move in the same transaction - if the audit write
// fails, the orders rows roll back with it. A loader holding its own Client escapes the transaction.
data_frame()
    ->read(from_csv(__DIR__ . '/data/orders.csv'))
    ->select('order_id', 'customer')
    ->limit(10)
    ->write(
        to_pgsql_transaction(
            $client,
            to_pgsql_table($client, 'orders'),
            to_pgsql_table($client, 'orders_audit'),
        )
    )
    ->run();

data_frame()
    ->read(
        from_pgsql_cursor(
            $client,
            select(literal('orders')->as('table'), count_all()->as('rows'))
                ->from(table('orders'))
                ->unionAll(
                    select(literal('orders_audit')->as('table'), count_all()->as('rows'))
                        ->from(table('orders_audit')),
                ),
        )
    )
    ->collect()
    ->write(to_output(truncate: false))
    ->run();
