<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\Datasets\Datasets;

use function Flow\ETL\Adapter\PostgreSql\from_pgsql_key_set;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_asc;
use function Flow\ETL\Adapter\PostgreSql\pgsql_pagination_key_set;
use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\DSL\data_frame;
use function Flow\Floe\DSL\from_floe;
use function Flow\PostgreSql\DSL\asc;
use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\star;
use function Flow\PostgreSql\DSL\table;

final readonly class PostgresqlReadScenario
{
    public function __construct(
        private int $rows,
    ) {}

    public function table(): string
    {
        return 'benchmark_orders_pgsql_read_' . $this->rows;
    }

    public function seed(): void
    {
        $client = PostgresqlConnection::open();
        PostgresqlConnection::dropTable($client, $this->table());
        PostgresqlConnection::createTable($client, $this->table());

        data_frame()
            ->read(from_floe(Datasets::orders($this->rows)->floe()))
            ->batchSize(1000)
            ->write(to_pgsql_table($client, $this->table()))
            ->run();

        $client->close();
    }

    public function run(): void
    {
        $client = PostgresqlConnection::open();

        data_frame()->read(from_pgsql_key_set(
            $client,
            select(star())->from(table($this->table()))->orderBy(asc(col('order_id'))),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('order_id')),
        )->withPageSize(1000))->run();

        $client->close();
    }

    public function dropTable(): void
    {
        $client = PostgresqlConnection::open();
        PostgresqlConnection::dropTable($client, $this->table());
        $client->close();
    }
}
