<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class ExecuteCommandTest extends CommandTestCase
{
    public function test_execute_cancelled_when_user_declines() : void
    {
        $version = $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.execute');
        $tester = new CommandTester($command);
        $tester->setInputs(['no']);
        $tester->execute(['version' => $version]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('cancelled', $tester->getDisplay());
        self::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_execute_skips_confirm_in_non_interactive_mode() : void
    {
        $version = $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.execute');
        $tester = new CommandTester($command);
        $tester->execute(['version' => $version], ['interactive' => false]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertTrue($this->context->tableExists('test_users'));
    }

    public function test_executes_migration_down() : void
    {
        $version = $this->context->generateDiffMigration();
        $this->context->runMigrate();

        self::assertTrue($this->context->tableExists('test_users'));

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.execute');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => $version, '--down' => true]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('DOWN', $tester->getDisplay());
        self::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_executes_migration_up() : void
    {
        $version = $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.execute');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => $version, '--up' => true]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('UP', $tester->getDisplay());
        self::assertStringContainsString($version, $tester->getDisplay());
        self::assertTrue($this->context->tableExists('test_users'));
    }
}
