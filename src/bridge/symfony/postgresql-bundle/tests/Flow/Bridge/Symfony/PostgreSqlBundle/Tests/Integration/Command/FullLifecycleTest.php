<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class FullLifecycleTest extends TestCase
{
    public function test_create_drop_create_diff_migrate_prev_next_latest(): void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_lifecycle_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            static::assertTrue($context->databaseExists($testDbName));

            $context->dropDatabase($testDbName);
            static::assertFalse($context->databaseExists($testDbName));

            $context->createDatabase($testDbName);
            static::assertTrue($context->databaseExists($testDbName));

            $context->bootWithMigrationsForDatabase($testDbName);

            $diffCommand = $context->container()->get('flow.postgresql.command.diff');
            $tester = new CommandTester($diffCommand);
            $tester->execute(['name' => 'initial_schema', '--from-empty-schema' => true]);
            static::assertSame(Command::SUCCESS, $tester->getStatusCode());
            static::assertStringContainsString('Generated migration', $tester->getDisplay());

            $migrateCommand = $context->container()->get('flow.postgresql.command.migrate');
            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute([]);
            static::assertSame(Command::SUCCESS, $tester->getStatusCode(), $tester->getDisplay());
            static::assertStringContainsString('UP', $tester->getDisplay());
            static::assertTrue($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute(['version' => 'prev']);
            static::assertSame(Command::SUCCESS, $tester->getStatusCode());
            static::assertStringContainsString('DOWN', $tester->getDisplay());
            static::assertFalse($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->setInputs(['yes']);
            $tester->execute(['version' => 'next']);
            static::assertSame(Command::SUCCESS, $tester->getStatusCode());
            static::assertStringContainsString('UP', $tester->getDisplay());
            static::assertTrue($context->tableExistsInDatabase($testDbName, 'test_users'));

            $tester = new CommandTester($migrateCommand);
            $tester->execute([]);
            static::assertSame(Command::SUCCESS, $tester->getStatusCode());
            static::assertStringContainsString('Already up to date', $tester->getDisplay());
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }

    public function test_migrate_with_no_migrations_available(): void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_empty_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            $context->bootWithMigrationsForDatabase($testDbName);

            $migrateCommand = $context->container()->get('flow.postgresql.command.migrate');
            $tester = new CommandTester($migrateCommand);
            $tester->execute([]);

            static::assertSame(Command::SUCCESS, $tester->getStatusCode());
            static::assertStringContainsString('No migrations found', $tester->getDisplay());
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }
}
