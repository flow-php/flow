<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLSession\SessionCatalogProvider;
use Flow\PostgreSql\Client\Client;
use Flow\PostgreSql\Client\ConnectionParameters;
use RuntimeException;

use function Flow\PostgreSql\DSL\col;
use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\eq;
use function Flow\PostgreSql\DSL\param;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;
use function Flow\PostgreSql\DSL\select;
use function Flow\PostgreSql\DSL\table;
use function Flow\PostgreSql\DSL\update;
use function getenv;

final readonly class SessionTestContext
{
    public Client $client;

    public ConnectionParameters $connectionParameters;

    public function __construct()
    {
        $dsn = getenv('PGSQL_DATABASE_URL');

        if (!$dsn) {
            throw new RuntimeException('PGSQL_DATABASE_URL environment variable is not set');
        }

        $this->connectionParameters = pgsql_connection_dsn($dsn);
        $this->client = pgsql_client($this->connectionParameters);
    }

    public function close(): void
    {
        $this->client->close();
    }

    public function createSessionTable(string $tableName = 'sessions', string $schemaName = 'public'): void
    {
        $provider = new SessionCatalogProvider($tableName, $schemaName);
        $table = $provider->get()->get($schemaName)->tables[0];

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }
    }

    public function dropSessionTable(string $tableName = 'sessions'): void
    {
        $this->client->execute(drop()->table($tableName)->ifExists()->cascade());
    }

    public function fetchSessionLifetime(
        string $sessionId,
        string $tableName = 'sessions',
        string $schemaName = 'public',
    ): int {
        return $this->client->fetchScalarInt(
            select(col('sess_lifetime'))->from(table($tableName, $schemaName))->where(eq(col('sess_id'), param(1))),
            [$sessionId],
        );
    }

    public function markSessionExpired(
        string $sessionId,
        string $tableName = 'sessions',
        string $schemaName = 'public',
    ): void {
        $this->client->execute(
            update()
                ->update(table($tableName, $schemaName))
                ->set('sess_lifetime', param(1))
                ->where(eq(col('sess_id'), param(2))),
            [1, $sessionId],
        );
    }
}
