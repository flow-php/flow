<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration;

use function Flow\PostgreSql\DSL\{drop, pgsql_client, pgsql_connection_dsn};

use Flow\PostgreSql\Client\Client;

final class PostgreSqlMigrationsContext
{
    public readonly Client $client;

    public function __construct()
    {
        $dsn = \getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new \RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    public function close() : void
    {
        $this->client->close();
    }

    public function dropTableIfExists(string $table) : void
    {
        $this->client->execute(drop()->table($table)->ifExists()->cascade()->toSql());
    }
}
