<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class ListCommandTest extends CommandTestCase
{
    public function test_lists_pending_migration() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.list');
        $tester = new CommandTester($command);
        $tester->execute([]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('PENDING', $tester->getDisplay());
        self::assertStringContainsString('create_test_users', $tester->getDisplay());
    }
}
