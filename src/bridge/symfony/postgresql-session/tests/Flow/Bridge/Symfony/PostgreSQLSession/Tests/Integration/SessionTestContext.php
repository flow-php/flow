<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLSession\Tests\Integration;

use function Flow\PostgreSql\DSL\{col, drop, eq, param, pgsql_client, pgsql_connection_dsn, select, table, update};

use Flow\Bridge\Symfony\PostgreSQLSession\SessionCatalogProvider;
use Flow\PostgreSql\Client\Client;

final readonly class SessionTestContext
{
    public Client $client;

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

    public function createSessionTable(string $tableName = 'sessions', string $schemaName = 'public') : void
    {
        $provider = new SessionCatalogProvider($tableName, $schemaName);
        $table = $provider->get()->get($schemaName)->tables[0];

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }
    }

    public function dropSessionTable(string $tableName = 'sessions') : void
    {
        $this->client->execute(drop()->table($tableName)->ifExists()->cascade());
    }

    public function fetchSessionLifetime(string $sessionId, string $tableName = 'sessions', string $schemaName = 'public') : int
    {
        return $this->client->fetchScalarInt(
            select(col('sess_lifetime'))
                ->from(table($tableName, $schemaName))
                ->where(eq(col('sess_id'), param(1))),
            [$sessionId],
        );
    }

    public function markSessionExpired(string $sessionId, string $tableName = 'sessions', string $schemaName = 'public') : void
    {
        $this->client->execute(
            update()
                ->update(table($tableName, $schemaName))
                ->set('sess_lifetime', param(1))
                ->where(eq(col('sess_id'), param(2))),
            [1, $sessionId],
        );
    }
}
