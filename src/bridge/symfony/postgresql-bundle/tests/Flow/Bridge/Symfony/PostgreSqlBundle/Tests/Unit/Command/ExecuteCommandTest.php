<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\ExecuteCommand;
use Flow\PostgreSql\Migrations\{Configuration, Migrator, Version};
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, FakeMigrationStore, SpyClient, SpyMigration, SpyMigrationExecutor, SpyRollback};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class ExecuteCommandTest extends TestCase
{
    public function test_dry_run_shows_warning() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, new FakeMigrationStore(), new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new ExecuteCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000', '--dry-run' => true]);

        self::assertStringContainsString('Dry run completed.', $tester->getDisplay());
    }

    public function test_executes_migration_down() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );

        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new ExecuteCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000', '--down' => true]);

        $display = $tester->getDisplay();
        self::assertStringContainsString('DOWN', $display);
        self::assertStringContainsString('20260401120000', $display);
    }

    public function test_executes_single_migration() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, new FakeMigrationStore(), new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new ExecuteCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000']);

        $display = $tester->getDisplay();
        self::assertStringContainsString('UP', $display);
        self::assertStringContainsString('20260401120000', $display);
    }
}
