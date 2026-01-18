<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Tests\Benchmark;

use function Flow\ETL\Adapter\PostgreSql\{from_pgsql_cursor, from_pgsql_key_set, from_pgsql_limit_offset, pgsql_pagination_key_asc, pgsql_pagination_key_set, to_pgsql_table};
use function Flow\ETL\DSL\{config, df, flow_context};
use function Flow\PostgreSql\DSL\{asc, col, column, create, data_type_double_precision, data_type_integer, data_type_jsonb, data_type_text, data_type_timestamptz, data_type_uuid, drop, pgsql_client, pgsql_connection_dsn, pgsql_mapper, select, star, table};
use Flow\ETL\Tests\Double\FakeStaticOrdersExtractor;
use Flow\PostgreSql\Client\Client;
use PhpBench\Attributes\Groups;

#[Groups(['extractor'])]
final class PostgreSqlExtractorBench
{
    private const TABLE_NAME = 'benchmark_orders_extractor';

    private Client $client;

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

        $this->setupDatabase();
    }

    public function __destruct()
    {
        $this->client->execute(
            drop()->table(self::TABLE_NAME)->ifExists()->cascade()
        );
        $this->client->close();
    }

    public function bench_extract_1k_cursor() : void
    {
        $context = flow_context(config());

        foreach (from_pgsql_cursor(
            $this->client,
            select(star())->from(table(self::TABLE_NAME)),
        )->withFetchSize(1000)->extract($context) as $rows) {
        }
    }

    public function bench_extract_1k_keyset() : void
    {
        $context = flow_context(config());

        foreach (from_pgsql_key_set(
            $this->client,
            select(star())->from(table(self::TABLE_NAME)),
            pgsql_pagination_key_set(pgsql_pagination_key_asc('index')),
        )->withPageSize(1000)->extract($context) as $rows) {
        }
    }

    public function bench_extract_1k_limit_offset() : void
    {
        $context = flow_context(config());

        foreach (from_pgsql_limit_offset(
            $this->client,
            select(star())->from(table(self::TABLE_NAME))->orderBy(asc(col('index'))),
        )->withPageSize(1000)->extract($context) as $rows) {
        }
    }

    private function setupDatabase() : void
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

        $extractor = new FakeStaticOrdersExtractor(1_000);
        $context = flow_context(config());

        df()
            ->read($extractor)
            ->write(to_pgsql_table($this->client, self::TABLE_NAME))
            ->run();
    }
}
