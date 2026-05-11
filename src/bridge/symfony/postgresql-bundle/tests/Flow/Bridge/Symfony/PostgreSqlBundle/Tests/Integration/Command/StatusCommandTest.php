<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class StatusCommandTest extends CommandTestCase
{
    public function test_shows_migration_status(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.status');
        $tester = new CommandTester($command);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        $display = $tester->getDisplay();
        static::assertStringContainsString('Total migrations', $display);
        static::assertStringContainsString('Pending', $display);
        static::assertStringContainsString('pending', $display);
    }
}
