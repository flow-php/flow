<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration\Store;

use Flow\PostgreSql\Migrations\{Configuration, Version};
use Flow\PostgreSql\Migrations\Store\PostgreSqlMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Integration\PostgreSqlMigrationsContext;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class PostgreSqlMigrationStoreTest extends TestCase
{
    protected Configuration $configuration;

    protected PostgreSqlMigrationsContext $context;

    protected PostgreSqlMigrationStore $store;

    protected function setUp() : void
    {
        $this->context = new PostgreSqlMigrationsContext();
        $this->configuration = new Configuration(
            client: $this->context->client,
            targetCatalogProvider: new FakeCatalogProvider(new Catalog([])),
            migrationsDirectory: __DIR__,
            migrationsNamespace: 'Flow\\PostgreSql\\Migrations\\Tests\\Integration\\Store',
            tableName: 'flow_migrations_test',
            tableSchema: 'public',
        );
        $this->store = new PostgreSqlMigrationStore($this->context->client, $this->configuration);

        $this->context->dropTableIfExists('public.flow_migrations_test');
    }

    protected function tearDown() : void
    {
        $this->context->dropTableIfExists('public.flow_migrations_test');
        $this->context->close();
    }

    public function test_complete_inserts_record() : void
    {
        $this->store->initialize();

        $version = Version::fromString('20260403120000');
        $this->store->complete($version, 150);

        $executed = $this->store->executedMigrations();

        self::assertCount(1, $executed);
        self::assertTrue($executed->has($version));
        self::assertSame(150, $executed->get($version)->executionTimeMs);
    }

    public function test_executed_migrations_returns_sorted_results() : void
    {
        $this->store->initialize();

        $this->store->complete(Version::fromString('20260403140000'), 300);
        $this->store->complete(Version::fromString('20260403120000'), 100);
        $this->store->complete(Version::fromString('20260403130000'), 200);

        $executed = $this->store->executedMigrations();

        $versions = [];

        foreach ($executed as $migration) {
            $versions[] = (string) $migration->version;
        }

        self::assertSame(['20260403120000', '20260403130000', '20260403140000'], $versions);
    }

    public function test_initialize_creates_table() : void
    {
        $this->store->initialize();

        self::assertTrue($this->store->isInitialized());
    }

    public function test_is_initialized_returns_false_before_init() : void
    {
        self::assertFalse($this->store->isInitialized());
    }

    public function test_is_initialized_returns_true_after_init() : void
    {
        self::assertFalse($this->store->isInitialized());

        $this->store->initialize();

        self::assertTrue($this->store->isInitialized());
    }

    public function test_remove_deletes_record() : void
    {
        $this->store->initialize();

        $version = Version::fromString('20260403120000');
        $this->store->complete($version, 150);

        self::assertCount(1, $this->store->executedMigrations());

        $this->store->remove($version);

        self::assertCount(0, $this->store->executedMigrations());
    }

    public function test_reset_clears_all_records() : void
    {
        $this->store->initialize();

        $this->store->complete(Version::fromString('20260403120000'), 100);
        $this->store->complete(Version::fromString('20260403130000'), 200);

        self::assertCount(2, $this->store->executedMigrations());

        $this->store->reset();

        self::assertCount(0, $this->store->executedMigrations());
        self::assertTrue($this->store->executedMigrations()->isEmpty());
    }
}
