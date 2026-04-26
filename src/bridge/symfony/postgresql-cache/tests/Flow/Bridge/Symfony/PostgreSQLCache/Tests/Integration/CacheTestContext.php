<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLCache\Tests\Integration;

use function Flow\PostgreSql\DSL\{col, drop, is_null, param, pgsql_client, pgsql_connection_dsn, table, update};

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

    /**
     * Make every row that has a non-null lifetime appear already-expired by
     * zeroing its time column. Rows with a null lifetime (i.e. saved without
     * an explicit TTL) are untouched and remain alive.
     */
    public function expireAllExpirableCacheItems(string $tableName = 'cache_items', string $schemaName = 'public') : void
    {
        $this->client->execute(
            update()
                ->update(table($tableName, $schemaName))
                ->set('item_time', param(1))
                ->where(is_null(col('item_lifetime'), not: true)),
            [0],
        );
    }
}
