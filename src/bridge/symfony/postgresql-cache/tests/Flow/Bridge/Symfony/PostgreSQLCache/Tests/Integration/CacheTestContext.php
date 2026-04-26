<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Integration;

use function Flow\PostgreSql\DSL\{drop, pgsql_client, pgsql_connection_dsn};

use Flow\Bridge\Symfony\PostgreSQLCache\CacheCatalogProvider;
use Flow\PostgreSql\Client\Client;

final readonly class CacheTestContext
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

    public function createCacheTable(string $tableName = 'cache_items', string $schemaName = 'public') : void
    {
        $provider = new CacheCatalogProvider($tableName, $schemaName);
        $table = $provider->get()->get($schemaName)->tables[0];

        foreach ($table->toSql() as $sql) {
            $this->client->execute($sql);
        }
    }

    public function dropCacheTable(string $tableName = 'cache_items') : void
    {
        $this->client->execute(drop()->table($tableName)->ifExists()->cascade());
    }
}
