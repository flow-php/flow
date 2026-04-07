<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class DropDatabaseCommandTest extends TestCase
{
    public function test_drop_requires_force() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_drop_' . \bin2hex(\random_bytes(4));

        try {
            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_drop');
            $tester = new CommandTester($command);
            $tester->execute([]);

            self::assertSame(Command::FAILURE, $tester->getStatusCode());
            self::assertStringContainsString('--force', $tester->getDisplay());
        } finally {
            $context->shutdown();
        }
    }

    public function test_drop_with_if_exists_succeeds_when_database_missing() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_drop_nonexistent_' . \bin2hex(\random_bytes(4));

        try {
            self::assertFalse($context->databaseExists($testDbName));

            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_drop');
            $tester = new CommandTester($command);
            $tester->execute(['--force' => true, '--if-exists' => true]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        } finally {
            $context->shutdown();
        }
    }

    public function test_drops_database() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_drop_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            self::assertTrue($context->databaseExists($testDbName));

            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_drop');
            $tester = new CommandTester($command);
            $tester->execute(['--force' => true]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString($testDbName, $tester->getDisplay());
            self::assertFalse($context->databaseExists($testDbName));
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }

    public function test_drops_postgres_database() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_postgres_drop_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            self::assertTrue($context->databaseExists($testDbName));

            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_drop');
            $tester = new CommandTester($command);
            $tester->execute(['--force' => true]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertFalse($context->databaseExists($testDbName));
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }
}
