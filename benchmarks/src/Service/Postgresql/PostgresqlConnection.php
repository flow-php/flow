<?php

declare(strict_types=1);

namespace Flow\Benchmarks\Service\Postgresql;

use Flow\Benchmarks\Service\Services;
use Flow\ETL\Tests\Double\FakeRandomOrdersExtractor;
use Flow\PostgreSql\Client\Client;
use RuntimeException;
use Throwable;

use function Flow\ETL\Adapter\PostgreSql\to_pgsql_schema_table;
use function Flow\PostgreSql\DSL\alter;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function Flow\PostgreSql\DSL\primary_key;

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

    /**
     * $keyed adds the primary key on order_id, the read scenarios' keyset: without its index every page scans and
     * sorts the whole table. The write scenarios leave it off - phpbench's warmup inserts the same rows twice per
     * iteration.
     */
    public static function createTable(Client $client, string $table, bool $keyed = false): void
    {
        foreach (to_pgsql_schema_table((new FakeRandomOrdersExtractor())->schema(), $table)->toSql() as $sql) {
            $client->execute($sql);
        }

        if ($keyed) {
            $client->execute(alter()->table($table)->addConstraint(primary_key('order_id')));
        }
    }

    public static function dropTable(Client $client, string $table): void
    {
        $client->execute(drop()->table($table)->ifExists()->cascade());
    }
}
