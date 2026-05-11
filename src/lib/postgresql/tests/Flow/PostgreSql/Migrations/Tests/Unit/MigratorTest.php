<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Migrations\Tests\Unit;

use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Direction;
use Flow\PostgreSql\Migrations\MigrationState;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationExecutor;
use Flow\PostgreSql\Migrations\Tests\Double\SpyRollback;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;

final class MigratorTest extends TestCase
{
    public function test_execute_version_down(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $result = $migrator->executeVersion(Version::fromString('20260401120000'), Direction::DOWN);

        static::assertTrue($result->isSuccessful());
        static::assertSame(Direction::DOWN, $result->direction);
        static::assertFalse($store->executedMigrations()->has(Version::fromString('20260401120000')));
    }

    public function test_execute_version_up(): void
    {
        $store = new FakeMigrationStore();
        $executor = new SpyMigrationExecutor();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            $store,
            $executor,
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $result = $migrator->executeVersion(Version::fromString('20260401120000'), Direction::UP);

        static::assertTrue($result->isSuccessful());
        static::assertCount(1, $executor->executedPlans);
        static::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
    }

    public function test_migrate_dry_run_does_not_store(): void
    {
        $store = new FakeMigrationStore();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $results = $migrator->migrate(dryRun: true);

        static::assertCount(1, $results);
        static::assertTrue($results[0]->isSuccessful());
        static::assertCount(0, $store->executedMigrations());
    }

    public function test_migrate_executes_pending_migrations(): void
    {
        $executor = new SpyMigrationExecutor();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
            $executor,
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $results = $migrator->migrate();

        static::assertCount(3, $results);
        static::assertCount(3, $executor->executedPlans);

        foreach ($results as $result) {
            static::assertTrue($result->isSuccessful());
            static::assertSame(Direction::UP, $result->direction);
        }
    }

    public function test_migrate_initializes_store(): void
    {
        $store = new FakeMigrationStore();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $migrator->migrate();

        static::assertTrue($store->isInitialized());
    }

    public function test_migrate_overrides_configuration_all_or_nothing(): void
    {
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            new Configuration(
                $client,
                new FakeCatalogProvider(new Catalog([])),
                '/tmp',
                'App\\Migrations',
                allOrNothing: true,
            ),
        );

        $migrator->migrate(allOrNothing: false);

        static::assertSame(0, $client->transactionCallCount);
    }

    public function test_migrate_removes_on_down(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $store->complete(Version::fromString('20260402120000'), 100);
        $store->complete(Version::fromString('20260403120000'), 100);
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'first',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
                new AvailableMigration(
                    Version::fromString('20260402120000'),
                    'second',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
                new AvailableMigration(
                    Version::fromString('20260403120000'),
                    'third',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $results = $migrator->migrate(Version::fromString('20260401120000'));

        static::assertCount(2, $results);
        static::assertCount(1, $store->executedMigrations());
        static::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
    }

    public function test_migrate_returns_empty_when_already_at_target(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        static::assertSame([], $migrator->migrate(Version::fromString('20260401120000')));
    }

    public function test_migrate_returns_empty_when_no_available(): void
    {
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        static::assertSame([], $migrator->migrate());
    }

    public function test_migrate_stores_completed_on_success(): void
    {
        $store = new FakeMigrationStore();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $migrator->migrate();

        static::assertCount(2, $store->executedMigrations());
        static::assertTrue($store->executedMigrations()->has(Version::fromString('20260401120000')));
        static::assertTrue($store->executedMigrations()->has(Version::fromString('20260402120000')));
    }

    public function test_migrate_up_to_specific_version(): void
    {
        $executor = new SpyMigrationExecutor();
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260403120000'), 'third', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
            $executor,
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $results = $migrator->migrate(Version::fromString('20260402120000'));

        static::assertCount(2, $results);
        static::assertCount(2, $executor->executedPlans);
    }

    public function test_migrate_uses_all_or_nothing_from_configuration(): void
    {
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            new Configuration(
                $client,
                new FakeCatalogProvider(new Catalog([])),
                '/tmp',
                'App\\Migrations',
                allOrNothing: true,
            ),
        );

        $migrator->migrate();

        static::assertSame(1, $client->transactionCallCount);
    }

    public function test_migrate_without_transaction_when_all_or_nothing_disabled(): void
    {
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            new Configuration(
                $client,
                new FakeCatalogProvider(new Catalog([])),
                '/tmp',
                'App\\Migrations',
                allOrNothing: false,
            ),
        );

        $migrator->migrate();

        static::assertSame(0, $client->transactionCallCount);
    }

    public function test_status_shows_pending_and_executed(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
                new AvailableMigration(Version::fromString('20260402120000'), 'second', new SpyMigration(), null),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $status = $migrator->status();

        static::assertCount(2, $status);
        static::assertCount(1, $status->executed());
        static::assertCount(1, $status->pending());
    }

    public function test_status_shows_unavailable(): void
    {
        $store = new FakeMigrationStore();
        $store->complete(Version::fromString('20260401120000'), 100);
        $store->complete(Version::fromString('20260499120000'), 100);
        $client = new SpyClient();

        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(Version::fromString('20260401120000'), 'first', new SpyMigration(), null),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $status = $migrator->status();

        static::assertCount(2, $status);
        static::assertCount(1, $status->executed());

        $items = \iterator_to_array($status);
        $unavailable = \array_filter($items, static fn($s) => $s->state === MigrationState::UNAVAILABLE);

        static::assertCount(1, $unavailable);
    }
}
