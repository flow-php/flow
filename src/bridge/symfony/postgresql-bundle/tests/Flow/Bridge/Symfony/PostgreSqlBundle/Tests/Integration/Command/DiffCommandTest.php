<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class DiffCommandTest extends CommandTestCase
{
    public function test_generates_schema_migration(): void
    {
        $command = $this->context->command('flow.postgresql.command.diff');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'create_test_users']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Generated migration:', $tester->getDisplay());

        $dirs = $this->context->migrationDirs('*_create_test_users');
        static::assertCount(1, $dirs);

        $migrationDir = (string) $dirs[0];
        static::assertTrue($this->context->fileExists($migrationDir . '/migration.php'));
        static::assertTrue($this->context->fileExists($migrationDir . '/rollback.php'));

        $migrationContent = $this->context->fileContent($migrationDir . '/migration.php');
        static::assertStringContainsString('CREATE TABLE', $migrationContent);
        static::assertStringContainsString('test_users', $migrationContent);
        static::assertStringContainsString('implements Migration', $migrationContent);

        $rollbackContent = $this->context->fileContent($migrationDir . '/rollback.php');
        static::assertStringContainsString('DROP TABLE', $rollbackContent);
        static::assertStringContainsString('implements Rollback', $rollbackContent);
    }
}
