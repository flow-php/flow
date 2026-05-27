<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class GenerateCommandTest extends CommandTestCase
{
    public function test_generates_blank_migration(): void
    {
        $command = $this->context->command('flow.postgresql.command.generate');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'add_email_index']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Generated migration:', $tester->getDisplay());

        $dirs = $this->context->migrationDirs('*_add_email_index');
        static::assertCount(1, $dirs);
        $migrationDir = (string) $dirs[0];
        static::assertTrue($this->context->fileExists($migrationDir . '/migration.php'));
        static::assertTrue($this->context->fileExists($migrationDir . '/rollback.php'));
        static::assertStringContainsString(
            'implements Migration',
            $this->context->fileContent($migrationDir . '/migration.php'),
        );
        static::assertStringContainsString(
            'implements Rollback',
            $this->context->fileContent($migrationDir . '/rollback.php'),
        );
    }
}
