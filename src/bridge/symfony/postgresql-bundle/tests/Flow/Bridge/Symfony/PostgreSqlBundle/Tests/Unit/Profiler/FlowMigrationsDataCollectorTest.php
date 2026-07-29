<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowMigrationsDataCollector;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Store\MigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationExecutor;
use Flow\PostgreSql\Migrations\Tests\Double\SpyRollback;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use RuntimeException;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

#[CoversClass(FlowMigrationsDataCollector::class)]
final class FlowMigrationsDataCollectorTest extends TestCase
{
    public function test_get_name_is_flow_postgresql_migrations(): void
    {
        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator(
            new FakeMigrationRepository(),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            $configuration,
        );

        static::assertSame(
            'flow_postgresql_migrations',
            (new FlowMigrationsDataCollector('default', $migrator, $configuration))->getName(),
        );
    }

    public function test_collects_executed_and_pending_with_execution_time(): void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                new SpyMigration(),
                new SpyRollback(),
            ),
            new AvailableMigration(
                Version::fromString('20260402100000'),
                'seed_data',
                new SpyMigration(),
                new SpyRollback(),
            ),
        );
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 15);

        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, $configuration);

        $collector = new FlowMigrationsDataCollector('default', $migrator, $configuration);
        $collector->collect(new Request(), new Response());

        static::assertSame('default', $collector->getConnection());
        static::assertSame(2, $collector->getTotalCount());
        static::assertSame(1, $collector->getExecutedCount());
        static::assertSame(1, $collector->getPendingCount());
        static::assertSame(0, $collector->getUnavailableCount());

        static::assertNull($collector->getError());
        $configurationData = $collector->getConfiguration();
        static::assertIsArray($configurationData);
        static::assertSame('flow_migrations', $configurationData['tableName']);

        $executed = $collector->getMigrations()[0];
        static::assertSame('20260401120000', $executed['version']);
        static::assertSame('create_users', $executed['name']);
        static::assertSame('executed', $executed['state']);
        static::assertNotNull($executed['executedAt']);
        static::assertSame(15, $executed['executionTimeMs']);

        $pending = $collector->getMigrations()[1];
        static::assertSame('pending', $pending['state']);
        static::assertNull($pending['executedAt']);
        static::assertNull($pending['executionTimeMs']);
    }

    public function test_reports_unavailable_migration(): void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 5);

        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator(
            new FakeMigrationRepository(),
            $store,
            new SpyMigrationExecutor(),
            $client,
            $configuration,
        );

        $collector = new FlowMigrationsDataCollector('default', $migrator, $configuration);
        $collector->collect(new Request(), new Response());

        static::assertSame(1, $collector->getUnavailableCount());
        static::assertSame('unavailable', $collector->getMigrations()[0]['state']);
    }

    public function test_collect_degrades_gracefully_when_status_fails(): void
    {
        $store = $this->createStub(MigrationStore::class);
        $store->method('executedMigrations')->willThrowException(new RuntimeException('connection failed'));

        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator(
            new FakeMigrationRepository(),
            $store,
            new SpyMigrationExecutor(),
            $client,
            $configuration,
        );

        $collector = new FlowMigrationsDataCollector('default', $migrator, $configuration);
        $collector->collect(new Request(), new Response());

        static::assertSame('connection failed', $collector->getError());
        static::assertNull($collector->getConfiguration());
        static::assertSame(0, $collector->getTotalCount());
        static::assertSame([], $collector->getMigrations());
    }

    public function test_collect_is_idempotent_within_a_request(): void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                new SpyMigration(),
                new SpyRollback(),
            ),
        );
        $store = new FakeMigrationStore();

        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator($repository, $store, new SpyMigrationExecutor(), $client, $configuration);

        $collector = new FlowMigrationsDataCollector('default', $migrator, $configuration);
        $collector->collect(new Request(), new Response());
        $store->complete(Version::fromString('20260401120000'), 5);
        $collector->collect(new Request(), new Response());

        static::assertSame(1, $collector->getPendingCount());
        static::assertSame(0, $collector->getExecutedCount());
    }

    public function test_reset_clears_data(): void
    {
        $client = new SpyClient();
        $configuration = new Configuration(
            $client,
            new FakeCatalogProvider(new Catalog([])),
            '/tmp/migrations',
            'App\\Migrations',
        );
        $migrator = new Migrator(
            new FakeMigrationRepository(),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client,
            $configuration,
        );

        $collector = new FlowMigrationsDataCollector('default', $migrator, $configuration);
        $collector->collect(new Request(), new Response());
        $collector->reset();

        static::assertSame([], $collector->getMigrations());
        static::assertSame(0, $collector->getTotalCount());
    }
}
