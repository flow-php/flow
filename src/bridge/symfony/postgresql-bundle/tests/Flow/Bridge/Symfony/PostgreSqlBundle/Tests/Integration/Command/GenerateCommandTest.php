<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class GenerateCommandTest extends CommandTestCase
{
    public function test_generates_blank_migration() : void
    {
        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.generate');
        $tester = new CommandTester($command);
        $tester->execute(['name' => 'add_email_index']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('Generated migration:', $tester->getDisplay());

        $dirs = $this->context->migrationDirs('*_add_email_index');
        self::assertCount(1, $dirs);
        self::assertTrue($this->context->fileExists($dirs[0] . '/migration.php'));
        self::assertTrue($this->context->fileExists($dirs[0] . '/rollback.php'));
        self::assertStringContainsString('implements Migration', $this->context->fileContent($dirs[0] . '/migration.php'));
        self::assertStringContainsString('implements Rollback', $this->context->fileContent($dirs[0] . '/rollback.php'));
    }
}
