<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class MigrateCommandTest extends CommandTestCase
{
    public function test_migrate_cancelled_when_user_declines() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['no']);
        $tester->execute([]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('cancelled', $tester->getDisplay());
        self::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_dry_run_does_not_apply_changes() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['--dry-run' => true]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('Dry run completed', $tester->getDisplay());
        self::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_first_rolls_back_to_first_version() : void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        self::assertTrue($this->context->tableExists('test_users'));

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'first']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('Already up to date', $tester->getDisplay());
    }

    public function test_migrate_latest_applies_all_pending() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute([]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('UP', $tester->getDisplay());
        self::assertTrue($this->context->tableExists('test_users'));
        self::assertTrue($this->context->tableExists('flow_migrations_test'));
    }

    public function test_migrate_latest_shows_up_to_date_when_nothing_pending() : void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute([]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('Already up to date', $tester->getDisplay());
    }

    public function test_migrate_next_applies_one_step() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'next']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('UP', $tester->getDisplay());
        self::assertTrue($this->context->tableExists('test_users'));
    }

    public function test_migrate_prev_rolls_back_last_migration() : void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        self::assertTrue($this->context->tableExists('test_users'));

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'prev']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('DOWN', $tester->getDisplay());
        self::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_skips_confirm_in_non_interactive_mode() : void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->execute([], ['interactive' => false]);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('UP', $tester->getDisplay());
        self::assertTrue($this->context->tableExists('test_users'));
    }
}
