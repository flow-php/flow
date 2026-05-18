<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Integration;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\Executor\DefaultMigrationExecutor;
use Flow\PostgreSql\Migrations\Migration;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Rollback;
use Flow\PostgreSql\Migrations\Store\PostgreSqlMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\FailingMigration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Throwable;

use function Flow\Types\DSL\type_instance_of;

final class MigratorTest extends TestCase
{
    protected Configuration $configuration;

    protected PostgreSqlMigrationsContext $context;

    protected PostgreSqlMigrationStore $store;

    protected function setUp(): void
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

    protected function tearDown(): void
    {
        $this->context->dropTableIfExists('public.flow_migrations_test');
        $this->context->dropTableIfExists('public.users');
        $this->context->close();
    }

    public function test_all_or_nothing_rolls_back_on_failure(): void
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
        } catch (Throwable) {
        }

        static::assertCount(0, $this->store->executedMigrations());
    }

    public function test_dry_run_does_not_persist(): void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate(Version::fromString('20260401120000'), dryRun: true);

        static::assertCount(1, $results);
        static::assertTrue($results[0]->isSuccessful());
        static::assertCount(0, $this->store->executedMigrations());
    }

    public function test_execute_version_runs_single_migration(): void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $result = $migrator->executeVersion(Version::fromString('20260401120000'), Direction::UP);

        static::assertTrue($result->isSuccessful());
        static::assertCount(1, $this->store->executedMigrations());
    }

    public function test_migrate_down_to_specific_version(): void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $migrator->migrate(Version::fromString('20260402100000'));

        static::assertCount(2, $this->store->executedMigrations());

        $results = $migrator->migrate(Version::fromString('20260401120000'));

        static::assertCount(1, $results);
        static::assertSame(Direction::DOWN, $results[0]->direction);
        static::assertCount(1, $this->store->executedMigrations());
    }

    public function test_migrate_up_from_empty_state(): void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate();

        static::assertCount(3, $results);

        foreach ($results as $result) {
            static::assertTrue($result->isSuccessful());
            static::assertSame(Direction::UP, $result->direction);
        }

        static::assertCount(3, $this->store->executedMigrations());
    }

    public function test_migrate_up_to_specific_version(): void
    {
        $migrator = new Migrator(
            $this->fixtureRepository(),
            $this->store,
            new DefaultMigrationExecutor(),
            $this->context->client,
            $this->configuration,
        );

        $results = $migrator->migrate(Version::fromString('20260402100000'));

        static::assertCount(2, $results);
        static::assertCount(2, $this->store->executedMigrations());
    }

    public function test_status_returns_correct_states(): void
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

        static::assertCount(3, $status);
        static::assertCount(1, $status->executed());
        static::assertCount(2, $status->pending());
    }

    private function fixtureRepository(): FakeMigrationRepository
    {
        $fixturesPath = __DIR__ . '/../Fixture/migrations';

        return new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                type_instance_of(Migration::class)->assert(
                    require $fixturesPath . '/20260401120000_create_users/migration.php',
                ),
                type_instance_of(Rollback::class)->assert(
                    require $fixturesPath . '/20260401120000_create_users/rollback.php',
                ),
            ),
            new AvailableMigration(
                Version::fromString('20260402100000'),
                'seed_data',
                type_instance_of(Migration::class)->assert(
                    require $fixturesPath . '/20260402100000_seed_data/migration.php',
                ),
                type_instance_of(Rollback::class)->assert(
                    require $fixturesPath . '/20260402100000_seed_data/rollback.php',
                ),
            ),
            new AvailableMigration(
                Version::fromString('20260403090000'),
                'add_column',
                type_instance_of(Migration::class)->assert(
                    require $fixturesPath . '/20260403090000_add_column/migration.php',
                ),
                null,
            ),
        );
    }
}
