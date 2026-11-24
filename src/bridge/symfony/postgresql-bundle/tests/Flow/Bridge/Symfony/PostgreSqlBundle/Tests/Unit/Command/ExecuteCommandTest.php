<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\ExecuteCommand;
use Flow\PostgreSql\Migrations\Configuration;
use Flow\PostgreSql\Migrations\Migrator;
use Flow\PostgreSql\Migrations\Repository\AvailableMigration;
use Flow\PostgreSql\Migrations\Tests\Double\FakeCatalogProvider;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationRepository;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigration;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationExecutor;
use Flow\PostgreSql\Migrations\Tests\Double\SpyMigrationLock;
use Flow\PostgreSql\Migrations\Tests\Double\SpyRollback;
use Flow\PostgreSql\Migrations\Version;
use Flow\PostgreSql\Schema\Catalog;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

use function preg_replace;

final class ExecuteCommandTest extends TestCase
{
    public function test_dry_run_shows_warning(): void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                new SpyMigration(),
                new SpyRollback(),
            ),
        );

        $migrator = new Migrator(
            $repository,
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client = new SpyClient(),
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $tester = new CommandTester(new ExecuteCommand($migrator));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000', '--dry-run' => true]);

        static::assertStringContainsString('Dry run completed.', $tester->getDisplay());
    }

    public function test_executes_migration_down(): void
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
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);

        $migrator = new Migrator(
            $repository,
            $store,
            new SpyMigrationExecutor(),
            $client = new SpyClient(),
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $tester = new CommandTester(new ExecuteCommand($migrator));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000', '--down' => true]);

        $display = $tester->getDisplay();
        static::assertStringContainsString('DOWN', $display);
        static::assertStringContainsString('20260401120000', $display);
    }

    public function test_executes_single_migration(): void
    {
        $repository = new FakeMigrationRepository(
            new AvailableMigration(
                Version::fromString('20260401120000'),
                'create_users',
                new SpyMigration(),
                new SpyRollback(),
            ),
        );

        $migrator = new Migrator(
            $repository,
            new FakeMigrationStore(),
            new SpyMigrationExecutor(),
            $client = new SpyClient(),
            new Configuration($client, new FakeCatalogProvider(new Catalog([])), '/tmp', 'App\\Migrations'),
        );

        $tester = new CommandTester(new ExecuteCommand($migrator));
        $tester->setInputs(['yes']);
        $tester->execute(['version' => '20260401120000']);

        $display = $tester->getDisplay();
        static::assertStringContainsString('UP', $display);
        static::assertStringContainsString('20260401120000', $display);
    }

    #[TestWith([false, false, '--up', 'Another process is running migrations (lock "testdb.public.flow_migrations").'])]
    #[TestWith([true, true, '--up', 'Migration "20260401120000" is already executed.'])]
    #[TestWith([true, false, '--down', 'Migration "20260401120000" is not executed, there is nothing to roll back.'])]
    public function test_refused_execution_prints_the_message_and_fails(
        bool $lockFree,
        bool $executed,
        string $direction,
        string $message,
    ): void {
        $store = new FakeMigrationStore();

        if ($executed) {
            $store->complete(Version::fromString('20260401120000'), 100);
        }

        $lock = new SpyMigrationLock();
        $lock->acquired = $lockFree;
        $client = new SpyClient();
        $migrator = new Migrator(
            new FakeMigrationRepository(
                new AvailableMigration(
                    Version::fromString('20260401120000'),
                    'create_users',
                    new SpyMigration(),
                    new SpyRollback(),
                ),
            ),
            $store,
            new SpyMigrationExecutor(),
            $client,
            new Configuration(
                $client,
                new FakeCatalogProvider(new Catalog([])),
                '/tmp',
                'App\\Migrations',
                lock: $lock,
            ),
        );

        $tester = new CommandTester(new ExecuteCommand($migrator));
        $tester->execute(['version' => '20260401120000', $direction => true], ['interactive' => false]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString($message, preg_replace('/\s+/', ' ', $tester->getDisplay()) ?? '');
    }
}
