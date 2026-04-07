<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class DiffCommandTest extends CommandTestCase
{
    public function test_generates_schema_migration() : void
    {
        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.diff');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'create_test_users']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('Generated migration:', $tester->getDisplay());

        $dirs = $this->context->migrationDirs('*_create_test_users');
        self::assertCount(1, $dirs);

        $migrationDir = $dirs[0];
        self::assertTrue($this->context->fileExists($migrationDir . '/migration.php'));
        self::assertTrue($this->context->fileExists($migrationDir . '/rollback.php'));

        $migrationContent = $this->context->fileContent($migrationDir . '/migration.php');
        self::assertStringContainsString('CREATE TABLE', $migrationContent);
        self::assertStringContainsString('test_users', $migrationContent);
        self::assertStringContainsString('implements Migration', $migrationContent);

        $rollbackContent = $this->context->fileContent($migrationDir . '/rollback.php');
        self::assertStringContainsString('DROP TABLE', $rollbackContent);
        self::assertStringContainsString('implements Rollback', $rollbackContent);
    }
}
