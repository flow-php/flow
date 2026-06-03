<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Executor\DefaultMigrationExecutor;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Store\PostgreSqlMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class CustomSchemaMigrationTest extends TestCase
{
    private const CUSTOM_SCHEMA = 'flow_custom_schema_test';

    private const TABLE_NAME = 'flow_migrations_test';

    protected PostgreSqlMigrationsContext $context;

    protected function setUp(): void
    {
        $this->context = new PostgreSqlMigrationsContext();
        $this->context->dropSchemaIfExists(self::CUSTOM_SCHEMA);
    }

    protected function tearDown(): void
    {
        $this->context->dropSchemaIfExists(self::CUSTOM_SCHEMA);
        $this->context->close();
    }

    public function test_migrate_creates_tracking_table_in_custom_schema_and_records_migration(): void
    {
        $configuration = new Configuration(
            client: $this->context->client,
            targetCatalogProvider: new FakeCatalogProvider(new Catalog([])),
            migrationsDirectory: __DIR__,
            migrationsNamespace: __NAMESPACE__,
            tableName: self::TABLE_NAME,
            tableSchema: self::CUSTOM_SCHEMA,
        );
        $store = new PostgreSqlMigrationStore($this->context->client, $configuration);

        static::assertFalse($store->isInitialized());

        $migration = new SpyMigration();
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260601120000'), 'noop', $migration, null),
        );
        $migrator = new Migrator(
            $repository,
            $store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $configuration,
        );

        $results = $migrator->migrate();

        static::assertCount(1, $results);
        static::assertTrue($results[0]->isSuccessful());
        static::assertSame(Direction::UP, $results[0]->direction);
        static::assertTrue($migration->migrateCalled);
        static::assertTrue($store->isInitialized());
        static::assertCount(1, $store->executedMigrations());
    }
}
