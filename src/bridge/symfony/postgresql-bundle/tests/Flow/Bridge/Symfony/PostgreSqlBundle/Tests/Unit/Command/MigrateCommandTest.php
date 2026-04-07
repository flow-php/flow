<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\MigrateCommand;
use Flow\PostgreSql\Migrations\{Configuration, Migrator, Version, VersionResolver};
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, FakeMigrationStore, SpyClient, SpyMigration, SpyMigrationExecutor, SpyRollback};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class MigrateCommandTest extends TestCase
{
    public function test_connection_option() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $container = new Container();
        $container->set('flow.postgresql.other.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));
        $container->set('flow.postgresql.other.migrations.version_resolver', new VersionResolver($repository, $store));
        $container->set('flow.postgresql.other.migrations.configuration', new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'));

        $tester = new CommandTester(new MigrateCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute(['--connection' => 'other']);

        self::assertStringContainsString('Already up to date.', $tester->getDisplay());
    }

    public function test_dry_run_note() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, new FakeMigrationStore(), new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));
        $container->set('flow.postgresql.default.migrations.version_resolver', new VersionResolver($repository, new FakeMigrationStore()));
        $container->set('flow.postgresql.default.migrations.configuration', new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'));

        $tester = new CommandTester(new MigrateCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute(['--dry-run' => true]);

        self::assertStringContainsString('Dry run completed.', $tester->getDisplay());
    }

    public function test_executes_pending_migrations() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402100000'), 'seed_data', new SpyMigration(), new SpyRollback()),
        );
        $store = new FakeMigrationStore();

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));
        $container->set('flow.postgresql.default.migrations.version_resolver', new VersionResolver($repository, $store));
        $container->set('flow.postgresql.default.migrations.configuration', new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'));

        $tester = new CommandTester(new MigrateCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute([]);

        $display = $tester->getDisplay();
        self::assertStringContainsString('UP', $display);
        self::assertStringContainsString('20260401120000', $display);
        self::assertStringContainsString('20260402100000', $display);
    }

    public function test_no_migrations_to_execute() : void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
        );
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));
        $container->set('flow.postgresql.default.migrations.version_resolver', new VersionResolver($repository, $store));
        $container->set('flow.postgresql.default.migrations.configuration', new Configuration(new SpyClient(), new FakeCatalogProvider(new Catalog([])), '/tmp/migrations', 'App\\Migrations'));

        $tester = new CommandTester(new MigrateCommand($container, 'default'));
        $tester->setInputs(['yes']);
        $tester->execute([]);

        self::assertStringContainsString('Already up to date.', $tester->getDisplay());
    }
}
