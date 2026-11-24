<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration;

use Flow\PostgreSql\Client\Client;
use RuntimeException;

use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function getenv;

final class PostgreSqlMigrationsContext
{
    public readonly Client $client;

    public readonly string $dsn;

    public ?Client $secondClient = null;

    public function __construct()
    {
        $dsn = getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->dsn = $dsn;
        $this->client = pgsql_client(pgsql_connection_dsn($dsn));
    }

    public function close(): void
    {
        $this->client->close();
        $this->secondClient?->close();
    }

    public function secondClient(): Client
    {
        return $this->secondClient ??= pgsql_client(pgsql_connection_dsn($this->dsn));
    }

    public function dropSchemaIfExists(string $schema): void
    {
        $this->client->execute(drop()->schema($schema)->ifExists()->cascade()->toSql());
    }

    public function dropTableIfExists(string $table): void
    {
        $this->client->execute(drop()->table($table)->ifExists()->cascade()->toSql());
    }
}
