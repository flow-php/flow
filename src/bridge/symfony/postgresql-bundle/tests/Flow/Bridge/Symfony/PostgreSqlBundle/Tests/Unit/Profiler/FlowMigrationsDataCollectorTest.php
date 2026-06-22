<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Profiler;

use Flow\Bridge\Symfony\PostgreSqlBundle\Profiler\FlowMigrationsDataCollector;
use Flow\PostgreSql\Migrations\Configuration;
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
use PHPUnit\Framework\Attributes\CoversClass;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Container;
use Symfony\Component\HttpFoundation\Request;
use Symfony\Component\HttpFoundation\Response;

use function array_keys;

#[CoversClass(FlowMigrationsDataCollector::class)]
final class FlowMigrationsDataCollectorTest extends TestCase
{
    public function test_get_name_is_flow_postgresql_migrations(): void
    {
        static::assertSame(
            'flow_postgresql_migrations',
            (new FlowMigrationsDataCollector(new Container(), []))->getName(),
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

        $container = new Container();
        $client = new SpyClient();
        $container->set(
            'flow.postgresql.default.migrations.migrator',
            new Migrator(
                $repository,
                $store,
                new SpyMigrationExecutor(),
                $client,
                new Configuration(
                    $client,
                    new FakeCatalogProvider(new Catalog([])),
                    '/tmp/migrations',
                    'App\\Migrations',
                ),
            ),
        );
        $container->set(
            'flow.postgresql.default.migrations.configuration',
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'),
        );

        $collector = new FlowMigrationsDataCollector($container, ['default']);
        $collector->collect(new Request(), new Response());

        static::assertSame(2, $collector->getTotalCount());
        static::assertSame(1, $collector->getExecutedCount());
        static::assertSame(1, $collector->getPendingCount());
        static::assertSame(0, $collector->getUnavailableCount());

        $connection = $collector->getConnections()['default'];
        static::assertNull($connection['error']);
        $configuration = $connection['configuration'];
        static::assertIsArray($configuration);
        static::assertSame('flow_migrations', $configuration['tableName']);

        $executed = $connection['migrations'][0];
        static::assertSame('20260401120000', $executed['version']);
        static::assertSame('create_users', $executed['name']);
        static::assertSame('executed', $executed['state']);
        static::assertNotNull($executed['executedAt']);
        static::assertSame(15, $executed['executionTimeMs']);

        $pending = $connection['migrations'][1];
        static::assertSame('pending', $pending['state']);
        static::assertNull($pending['executedAt']);
        static::assertNull($pending['executionTimeMs']);
    }

    public function test_reports_unavailable_migration(): void
    {
        $repository = new FakeMigrationRepository();
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 5);

        $container = new Container();
        $client = new SpyClient();
        $container->set(
            'flow.postgresql.default.migrations.migrator',
            new Migrator(
                $repository,
                $store,
                new SpyMigrationExecutor(),
                $client,
                new Configuration(
                    $client,
                    new FakeCatalogProvider(new Catalog([])),
                    '/tmp/migrations',
                    'App\\Migrations',
                ),
            ),
        );
        $container->set(
            'flow.postgresql.default.migrations.configuration',
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'),
        );

        $collector = new FlowMigrationsDataCollector($container, ['default']);
        $collector->collect(new Request(), new Response());

        static::assertSame(1, $collector->getUnavailableCount());
        static::assertSame('unavailable', $collector->getConnections()['default']['migrations'][0]['state']);
    }

    public function test_aggregates_across_multiple_connections(): void
    {
        $container = new Container();
        $client = new SpyClient();

        foreach (['default', 'analytics'] as $name) {
            $repository = new FakeMigrationRepository(
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'create_users',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            );
            $container->set(
                "flow.postgresql.{$name}.migrations.migrator",
                new Migrator(
                    $repository,
                    new FakeMigrationStore(),
                    new SpyMigrationExecutor(),
                    $client,
                    new Configuration(
                        $client,
                        new FakeCatalogProvider(new Catalog([])),
                        '/tmp/migrations',
                        'App\\Migrations',
                    ),
                ),
            );
            $container->set(
                "flow.postgresql.{$name}.migrations.configuration",
                new Configuration(
                    $client,
                    new FakeCatalogProvider(new Catalog([])),
                    '/tmp/migrations',
                    'App\\Migrations',
                ),
            );
        }

        $collector = new FlowMigrationsDataCollector($container, ['default', 'analytics']);
        $collector->collect(new Request(), new Response());

        static::assertSame(['default', 'analytics'], array_keys($collector->getConnections()));
        static::assertSame(2, $collector->getPendingCount());
        static::assertSame(2, $collector->getTotalCount());
    }

    public function test_collect_degrades_gracefully_when_connection_fails(): void
    {
        $collector = new FlowMigrationsDataCollector(new Container(), ['default']);
        $collector->collect(new Request(), new Response());

        $connection = $collector->getConnections()['default'];
        static::assertNotNull($connection['error']);
        static::assertNull($connection['configuration']);
        static::assertSame(0, $collector->getTotalCount());
        static::assertSame([], $connection['migrations']);
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

        $container = new Container();
        $client = new SpyClient();
        $container->set(
            'flow.postgresql.default.migrations.migrator',
            new Migrator(
                $repository,
                $store,
                new SpyMigrationExecutor(),
                $client,
                new Configuration(
                    $client,
                    new FakeCatalogProvider(new Catalog([])),
                    '/tmp/migrations',
                    'App\\Migrations',
                ),
            ),
        );
        $container->set(
            'flow.postgresql.default.migrations.configuration',
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'),
        );

        $collector = new FlowMigrationsDataCollector($container, ['default']);
        $collector->collect(new Request(), new Response());
        $store->complete(Version::fromString('20260401120000'), 5);
        $collector->collect(new Request(), new Response());

        static::assertSame(1, $collector->getPendingCount());
        static::assertSame(0, $collector->getExecutedCount());
    }

    public function test_reset_clears_data(): void
    {
        $collector = new FlowMigrationsDataCollector(new Container(), ['default']);
        $collector->collect(new Request(), new Response());
        $collector->reset();

        static::assertSame([], $collector->getConnections());
        static::assertSame(0, $collector->getTotalCount());
    }
}
