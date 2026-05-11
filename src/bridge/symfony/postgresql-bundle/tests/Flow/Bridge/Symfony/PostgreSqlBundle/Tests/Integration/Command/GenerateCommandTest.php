<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class GenerateCommandTest extends CommandTestCase
{
    public function test_generates_blank_migration(): void
    {
        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.generate');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'add_email_index']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Generated migration:', $tester->getDisplay());

        $dirs = $this->context->migrationDirs('*_add_email_index');
        static::assertCount(1, $dirs);
        static::assertTrue($this->context->fileExists($dirs[0] . '/migration.php'));
        static::assertTrue($this->context->fileExists($dirs[0] . '/rollback.php'));
        static::assertStringContainsString(
            'implements Migration',
            $this->context->fileContent($dirs[0] . '/migration.php'),
        );
        static::assertStringContainsString(
            'implements Rollback',
            $this->context->fileContent($dirs[0] . '/rollback.php'),
        );
    }
}
