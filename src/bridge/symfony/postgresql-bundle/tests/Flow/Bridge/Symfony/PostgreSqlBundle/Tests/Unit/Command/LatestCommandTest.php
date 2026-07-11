<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\LatestCommand;
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
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class LatestCommandTest extends TestCase
{
    public function test_no_migrations_message(): void
    {
        $migrator = new Migrator(
            new FakeMigrationRepository(),
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client = new SpyClient(),
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $tester = new CommandTester(new LatestCommand($migrator));
        $tester->execute([]);

        static::assertStringContainsString('No migrations available.', $tester->getDisplay());
    }

    public function test_shows_latest_version_with_marker(): void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

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

        $migrator = new Migrator(
            $repository,
            $store,
            new SpyMigrationExecutor(),
            $client = new SpyClient(),
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $tester = new CommandTester(new LatestCommand($migrator));
        $tester->execute([]);

        $display = $tester->getDisplay();
        static::assertStringContainsString('20260401120000', $display);
        static::assertStringContainsString('create_users', $display);
        static::assertStringContainsString('20260402100000', $display);
        static::assertStringContainsString('seed_data', $display);
        static::assertStringContainsString('EXECUTED', $display);
        static::assertStringContainsString('PENDING', $display);
    }
}
