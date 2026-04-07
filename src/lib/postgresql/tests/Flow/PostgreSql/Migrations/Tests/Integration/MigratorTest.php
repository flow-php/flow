<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration;

use Flow\PostgreSql\Migrations\{Configuration, Direction, Migrator, Version};
use Flow\PostgreSql\Migrations\Executor\DefaultMigrationExecutor;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Store\PostgreSqlMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\{FailingMigration, FakeCatalogProvider, FakeMigrationRepository, SpyMigration};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class MigratorTest extends TestCase
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
            migrationsDirectory: __DIR__ . '/../Fixture/migrations',
            migrationsNamespace: 'Flow\\PostgreSql\\Migrations\\Tests\\Fixture',
            tableName: 'flow_migrations_test',
            tableSchema: 'public',
        );
        $this->store = new PostgreSqlMigrationStore($this->context->client, $this->configuration);

        $this->context->dropTableIfExists('public.flow_migrations_test');
        $this->context->dropTableIfExists('public.users');
    }

    protected function tearDown() : void
    {
        $this->context->dropTableIfExists('public.flow_migrations_test');
        $this->context->dropTableIfExists('public.users');
        $this->context->close();
    }

    public function test_all_or_nothing_rolls_back_on_failure() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260501120000'), 'good', new SpyMigration(), null),
            new AvailableMigration(Version::fromString('20260502120000'), 'bad', new FailingMigration(), null),
        );

        $migrator = new Migrator(
            $repository,
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        try {
            $migrator->migrate(allOrNothing: true);
        } catch (\Throwable) {
        }

        self::assertCount(0, $this->store->executedMigrations());
    }

    public function test_dry_run_does_not_persist() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate(
            Version::fromString('20260401120000'),
            dryRun: true,
        );

        self::assertCount(1, $results);
        self::assertTrue($results[0]->isSuccessful());
        self::assertCount(0, $this->store->executedMigrations());
    }

    public function test_execute_version_runs_single_migration() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $result = $migrator->executeVersion(Version::fromString('20260401120000'), Direction::UP);

        self::assertTrue($result->isSuccessful());
        self::assertCount(1, $this->store->executedMigrations());
    }

    public function test_migrate_down_to_specific_version() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $migrator->migrate(Version::fromString('20260402100000'));

        self::assertCount(2, $this->store->executedMigrations());

        $results = $migrator->migrate(Version::fromString('20260401120000'));

        self::assertCount(1, $results);
        self::assertSame(Direction::DOWN, $results[0]->direction);
        self::assertCount(1, $this->store->executedMigrations());
    }

    public function test_migrate_up_from_empty_state() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate();

        self::assertCount(3, $results);

        foreach ($results as $result) {
            self::assertTrue($result->isSuccessful());
            self::assertSame(Direction::UP, $result->direction);
        }

        self::assertCount(3, $this->store->executedMigrations());
    }

    public function test_migrate_up_to_specific_version() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate(Version::fromString('20260402100000'));

        self::assertCount(2, $results);
        self::assertCount(2, $this->store->executedMigrations());
    }

    public function test_status_returns_correct_states() : void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $migrator->migrate(Version::fromString('20260401120000'));

        $status = $migrator->status();

        self::assertCount(3, $status);
        self::assertCount(1, $status->executed());
        self::assertCount(2, $status->pending());
    }

    private function fixtureRepository() : FakeMigrationRepository
    {
        $fixturesPath = __DIR__ . '/../Fixture/migrations';

        return new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                require $fixturesPath . '/20260401120000_create_users/migration.php',
                require $fixturesPath . '/20260401120000_create_users/rollback.php',
            ),
            new AvailableMigration(
                Version::fromString('20260402100000'),
                'seed_data',
                require $fixturesPath . '/20260402100000_seed_data/migration.php',
                require $fixturesPath . '/20260402100000_seed_data/rollback.php',
            ),
            new AvailableMigration(
                Version::fromString('20260403090000'),
                'add_column',
                require $fixturesPath . '/20260403090000_add_column/migration.php',
                null,
            ),
        );
    }
}
