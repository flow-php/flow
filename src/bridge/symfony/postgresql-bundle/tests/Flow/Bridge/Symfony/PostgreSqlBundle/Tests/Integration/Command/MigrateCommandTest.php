<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class MigrateCommandTest extends CommandTestCase
{
    public function test_migrate_cancelled_when_user_declines(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['no']);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('cancelled', $tester->getDisplay());
        static::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_dry_run_does_not_apply_changes(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['--dry-run' => true]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Dry run completed', $tester->getDisplay());
        static::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_first_rolls_back_to_first_version(): void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        static::assertTrue($this->context->tableExists('test_users'));

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'first']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Already up to date', $tester->getDisplay());
    }

    public function test_migrate_latest_applies_all_pending(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('UP', $tester->getDisplay());
        static::assertTrue($this->context->tableExists('test_users'));
        static::assertTrue($this->context->tableExists('flow_migrations_test'));
    }

    public function test_migrate_latest_shows_up_to_date_when_nothing_pending(): void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute([]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('Already up to date', $tester->getDisplay());
    }

    public function test_migrate_next_applies_one_step(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'next']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('UP', $tester->getDisplay());
        static::assertTrue($this->context->tableExists('test_users'));
    }

    public function test_migrate_prev_rolls_back_last_migration(): void
    {
        $this->context->generateDiffMigration();
        $this->context->runMigrate();

        static::assertTrue($this->context->tableExists('test_users'));

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->setInputs(['yes']);
        $tester->execute(['version' => 'prev']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('DOWN', $tester->getDisplay());
        static::assertFalse($this->context->tableExists('test_users'));
    }

    public function test_migrate_skips_confirm_in_non_interactive_mode(): void
    {
        $this->context->generateDiffMigration();

        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.migrate');
        $tester = new CommandTester($command);
        $tester->execute([], ['interactive' => false]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('UP', $tester->getDisplay());
        static::assertTrue($this->context->tableExists('test_users'));
    }
}
