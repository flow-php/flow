<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class CurrentCommandTest extends CommandTestCase
{
    public function test_shows_current_version_after_migration(): void
    {
        $version = $this->context->generateDiffMigration();
        $this->context->runMigrate();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.current');
        $tester = new CommandTester($command);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString($version, $tester->getDisplay());
        static::assertTrue($this->context->tableExists('flow_migrations_test'));
        static::assertTrue($this->context->tableExists('test_users'));
    }
}
