<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\ListCommand;
use Flow\PostgreSql\Migrations\{Configuration, Migrator, Version};
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\{FakeCatalogProvider, FakeMigrationRepository, FakeMigrationStore, SpyClient, SpyMigration, SpyMigrationExecutor, SpyRollback};
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class ListCommandTest extends TestCase
{
    public function test_empty_list() : void
    {
        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator(new FakeMigrationRepository(), new FakeMigrationStore(), new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new ListCommand($container, 'default'));
        $tester->execute([]);

        self::assertStringContainsString('No migrations found', $tester->getDisplay());
    }

    public function test_lists_migrations_with_states() : void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $repository = new FakeMigrationRepository(
            new AvailableMigration(Version::fromString('20260401120000'), 'create_users', new SpyMigration(), new SpyRollback()),
            new AvailableMigration(Version::fromString('20260402100000'), 'seed_data', new SpyMigration(), new SpyRollback()),
        );

        $container = new Container();
        $container->set('flow.postgresql.default.migrations.migrator', new Migrator($repository, $store, new SpyMigrationExecutor(), $client = new SpyClient(), new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations')));

        $tester = new CommandTester(new ListCommand($container, 'default'));
        $tester->execute([]);

        $display = $tester->getDisplay();
        self::assertStringContainsString('EXECUTED', $display);
        self::assertStringContainsString('20260401120000', $display);
        self::assertStringContainsString('create_users', $display);
        self::assertStringContainsString('PENDING', $display);
        self::assertStringContainsString('20260402100000', $display);
        self::assertStringContainsString('seed_data', $display);
        self::assertStringContainsString('1 executed', $display);
        self::assertStringContainsString('1 pending', $display);
    }
}
