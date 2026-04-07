<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class CreateDatabaseCommandTest extends TestCase
{
    public function test_create_fails_when_database_already_exists() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_create_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_create');
            $tester = new CommandTester($command);
            $tester->execute([]);

            self::assertSame(Command::FAILURE, $tester->getStatusCode());
            self::assertStringContainsString('already exists', $tester->getDisplay());
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }

    public function test_create_with_if_not_exists_succeeds_when_database_already_exists() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_create_' . \bin2hex(\random_bytes(4));

        try {
            $context->createDatabase($testDbName);
            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_create');
            $tester = new CommandTester($command);
            $tester->execute(['--if-not-exists' => true]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertTrue($context->databaseExists($testDbName));
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }

    public function test_creates_database() : void
    {
        $context = new CommandTestContext();
        $testDbName = 'flow_test_create_' . \bin2hex(\random_bytes(4));

        try {
            $context->bootForDatabaseManagement($testDbName);

            /** @var Command $command */
            $command = $context->container()->get('flow.postgresql.command.database_create');
            $tester = new CommandTester($command);
            $tester->execute([]);

            self::assertSame(Command::SUCCESS, $tester->getStatusCode());
            self::assertStringContainsString($testDbName, $tester->getDisplay());
            self::assertTrue($context->databaseExists($testDbName));
        } finally {
            $context->dropDatabase($testDbName);
            $context->shutdown();
        }
    }
}
