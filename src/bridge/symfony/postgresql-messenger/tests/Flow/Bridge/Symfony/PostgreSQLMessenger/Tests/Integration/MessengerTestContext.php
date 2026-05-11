<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Integration;

use Flow\Bridge\Symfony\PostgreSQLMessenger\MessengerCatalogProvider;
use Flow\PostgreSql\Client\Client;

use function Flow\PostgreSql\DSL\drop;
use function Flow\PostgreSql\DSL\pgsql_client;
use function Flow\PostgreSql\DSL\pgsql_connection_dsn;

final readonly class MessengerTestContext
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

    public function close(): void
    {
        $this->client->close();
    }

    public function createMessengerTable(string $tableName = 'messenger_messages', string $schemaName = 'public'): void
    {
        $provider = new MessengerCatalogProvider($tableName, $schemaName);
        $table = $provider->get()->get($schemaName)->tables[0];

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }
    }

    public function dropMessengerTable(string $tableName = 'messenger_messages'): void
    {
        $this->client->execute(drop()->table($tableName)->ifExists()->cascade());
    }
}
