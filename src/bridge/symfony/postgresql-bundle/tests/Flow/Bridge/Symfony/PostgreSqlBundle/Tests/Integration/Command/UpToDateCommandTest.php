<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class UpToDateCommandTest extends CommandTestCase
{
    public function test_returns_failure_when_pending(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.up_to_date');
        $tester = new CommandTester($command);
        $tester->execute([]);

        static::assertSame(Command::FAILURE, $tester->getStatusCode());
        static::assertStringContainsString('1 pending', $tester->getDisplay());
    }

    public function test_returns_success_when_up_to_date(): void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.up_to_date');
        $tester = new CommandTester($command);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('up to date', $tester->getDisplay());
    }
}
