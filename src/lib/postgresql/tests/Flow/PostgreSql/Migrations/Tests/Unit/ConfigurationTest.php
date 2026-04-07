<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, SpyClient};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class ConfigurationTest extends TestCase
{
    public function test_custom_values() : void
    {
        $client = new SpyClient();
        $catalog = new FakeCatalogProvider(new Catalog([]));

        $config = new Configuration(
            client: $client,
            targetCatalogProvider: $catalog,
            migrationsDirectory: '/app/migrations',
            migrationsNamespace: 'App\\Migrations',
            tableName: 'custom_migrations',
            tableSchema: 'app',
            allOrNothing: false,
            generateRollback: false,
        );

        self::assertSame($client, $config->client);
        self::assertSame($catalog, $config->targetCatalogProvider);
        self::assertSame('/app/migrations', $config->migrationsDirectory);
        self::assertSame('App\\Migrations', $config->migrationsNamespace);
        self::assertSame('custom_migrations', $config->tableName);
        self::assertSame('app', $config->tableSchema);
        self::assertFalse($config->allOrNothing);
        self::assertFalse($config->generateRollback);
    }

    public function test_default_values() : void
    {
        $config = new Configuration(
            client: new SpyClient(),
            targetCatalogProvider: new FakeCatalogProvider(new Catalog([])),
            migrationsDirectory: '/app/migrations',
            migrationsNamespace: 'App\\Migrations',
        );

        self::assertSame('flow_migrations', $config->tableName);
        self::assertSame('public', $config->tableSchema);
        self::assertFalse($config->allOrNothing);
        self::assertTrue($config->generateRollback);
    }
}
