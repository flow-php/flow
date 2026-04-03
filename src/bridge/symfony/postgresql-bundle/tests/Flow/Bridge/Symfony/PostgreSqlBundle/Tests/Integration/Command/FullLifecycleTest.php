<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class FullLifecycleTest extends TestCase
{
    public function test_create_drop_create_diff_migrate_prev_next_latest() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_lifecycle_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            self::assertTrue($context->databaseExists($testDbName));

            $context->dropDatabase($testDbName);
            self::assertFalse($context->databaseExists($testDbName));

            $context->createDatabase($testDbName);
            self::assertTrue($context->databaseExists($testDbName));

            $context->bootWithMigrationsForDatabase($testDbName);

            $diffCommand = $context->container()->get('flow.postgresql.command.diff');
            $tester = new CommandTester($diffCommand);
            $tester->execute(['name' => 'initial_schema', '--from-empty-schema' => true]);
            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString('Generated migration', $tester->getDisplay());

            $migrateCommand = $context->container()->get('flow.postgresql.command.migrate');
            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute([]);
            self::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
            self::assertStringContainsString('UP', $tester->getDisplay());
            self::assertTrue($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute(['version' => 'prev']);
            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString('DOWN', $tester->getDisplay());
            self::assertFalse($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute(['version' => 'next']);
            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString('UP', $tester->getDisplay());
            self::assertTrue($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->execute([]);
            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString('Already up to date', $tester->getDisplay());
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }

    public function test_migrate_with_no_migrations_available() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_empty_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            $context->bootWithMigrationsForDatabase($testDbName);

            $migrateCommand = $context->container()->get('flow.postgresql.command.migrate');
            $tester = new CommandTester($migrateCommand);
            $tester->execute([]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString('No migrations found', $tester->getDisplay());
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }
}
