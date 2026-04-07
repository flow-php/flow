<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\StatusCommand;
use Flow\PostgreSql\Migrations\{Configuration, Migrator, Version};
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, FakeMigrationStore, SpyClient, SpyMigration, SpyMigrationExecutor, SpyRollback};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class StatusCommandTest extends TestCase
{
    public function test_shows_counts_with_pending() : void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402100000'), 'seed_data', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260403090000'), 'add_column', new SpyMigration(), null),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new StatusCommand($container, 'default'));
        $tester->execute([]);

        $display = $tester->getDisplay();
        self::assertStringContainsString('3', $display);
        self::assertStringContainsString('1', $display);
        self::assertStringContainsString('2', $display);
        self::assertStringContainsString('pending', $display);
    }

    public function test_shows_success_when_all_executed() : void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new StatusCommand($container, 'default'));
        $tester->execute([]);

        self::assertStringContainsString('up to date', $tester->getDisplay());
    }
}
