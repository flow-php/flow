<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\Service\Services;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\PostgreSql\Client\Client;
use RuntimeException;
use Throwable;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;

final class PostgresqlConnection
{
    public static function open(): Client
    {
        if (!extension_loaded('pgsql') || !extension_loaded('pg_query')) {
            throw new RuntimeException(
                'PostgreSQL benchmarks require ext-pgsql and ext-pg_query. Run inside nix-shell (and install flow-php/pg-query-ext via PIE).',
            );
        }

        $dsn = Services::pgsqlDsn();

        try {
            return pgsql_client(pgsql_connection_dsn($dsn));
        } catch (Throwable $e) {
            throw new RuntimeException(
                'Cannot connect to PostgreSQL at '
                    . $dsn
                    . '. Start the docker compose services (docker compose up -d postgres) or override PGSQL_DATABASE_URL. Original error: '
                    . $e->getMessage(),
                previous: $e,
            );
        }
    }

    public static function createTable(Client $client, string $table): void
    {
        foreach (to_pgsql_schema_table((new FakeRandomOrdersExtractor())->schema(), $table)->toSql() as $sql) {
            $client->execute($sql);
        }
    }

    public static function dropTable(Client $client, string $table): void
    {
        $client->execute(drop()->table($table)->ifExists()->cascade());
    }
}
