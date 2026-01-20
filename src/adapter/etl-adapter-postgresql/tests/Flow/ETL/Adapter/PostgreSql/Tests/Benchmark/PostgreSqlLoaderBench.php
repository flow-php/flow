<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Benchmark;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_table;
use function Flow\ETL\DSL\flow_context;
use function Flow\PostgreSql\DSL\{begin, column, create, data_type_double_precision, data_type_integer, data_type_jsonb, data_type_text, data_type_timestamptz, data_type_uuid, drop, pgsql_client, pgsql_connection_dsn, pgsql_mapper, rollback};
use Flow\ETL\{FlowContext, Rows};
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use Flow\PostgreSql\Client\Client;
use PhpBench\Attributes\{AfterMethods, BeforeMethods, Groups};

#[Groups(['adapter-postgresql'])]
final class PostgreSqlLoaderBench
{
    private const TABLE_NAME = 'benchmark_orders_loader';

    private Client $client;

    private FlowContext $context;

    private Rows $rows;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        if (!\extension_loaded('pgsql')) {
            throw new \RuntimeException('ext-pgsql is not available');
        }

        if (!\extension_loaded('pg_query')) {
            throw new \RuntimeException('ext-pg_query is not available');
        }

        $this->client = pgsql_client(
            pgsql_connection_dsn($dsn),
            mapper: pgsql_mapper(),
        );
        $this->rows = (new FakeStaticOrdersExtractor(1_000))->toRows();
        $this->context = flow_context();
    }

    public function __destruct()
    {
        $this->client->execute(
            drop()->table(self::TABLE_NAME)->ifExists()->cascade()
        );
        $this->client->close();
    }

    public function setUp() : void
    {
        $this->client->execute(
            drop()->table(self::TABLE_NAME)->ifExists()->cascade()
        );

        $this->client->execute(
            create()->table(self::TABLE_NAME)
                ->column(column('index', data_type_integer())->primaryKey())
                ->column(column('order_id', data_type_uuid()))
                ->column(column('created_at', data_type_timestamptz()))
                ->column(column('updated_at', data_type_timestamptz()))
                ->column(column('discount', data_type_double_precision()))
                ->column(column('email', data_type_text()))
                ->column(column('customer', data_type_text()))
                ->column(column('address', data_type_jsonb()))
                ->column(column('notes', data_type_jsonb()))
                ->column(column('items', data_type_jsonb()))
        );
    }

    public function tearDown() : void
    {
        $this->client->execute(
            drop()->table(self::TABLE_NAME)->ifExists()->cascade()
        );
    }

    #[BeforeMethods('setUp')]
    #[AfterMethods('tearDown')]
    public function bench_load_1k() : void
    {
        $this->client->execute(begin());

        foreach ($this->rows->chunks(1_000) as $chunk) {
            to_pgsql_table($this->client, self::TABLE_NAME)->load($chunk, $this->context);
        }

        $this->client->execute(rollback());
    }
}
