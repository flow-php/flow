<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class ConfigurationTest extends TestCase
{
    public function test_custom_values(): void
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

        static::assertSame($client, $config->client);
        static::assertSame($catalog, $config->targetCatalogProvider);
        static::assertSame('/app/migrations', $config->migrationsDirectory);
        static::assertSame('App\\Migrations', $config->migrationsNamespace);
        static::assertSame('custom_migrations', $config->tableName);
        static::assertSame('app', $config->tableSchema);
        static::assertFalse($config->allOrNothing);
        static::assertFalse($config->generateRollback);
    }

    public function test_default_values(): void
    {
        $config = new Configuration(
            client: new SpyClient(),
            targetCatalogProvider: new FakeCatalogProvider(new Catalog([])),
            migrationsDirectory: '/app/migrations',
            migrationsNamespace: 'App\\Migrations',
        );

        static::assertSame('flow_migrations', $config->tableName);
        static::assertSame('public', $config->tableSchema);
        static::assertFalse($config->allOrNothing);
        static::assertTrue($config->generateRollback);
    }
}
