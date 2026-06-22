<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\CurrentCommand;
use Flow\PostgreSql\Migrations\Tests\Double\FakeMigrationStore;
use Flow\PostgreSql\Migrations\Version;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Tester\CommandTester;

final class CurrentCommandTest extends TestCase
{
    public function test_no_migrations_message(): void
    {
        $tester = new CommandTester(new CurrentCommand(new FakeMigrationStore()));
        $tester->execute([]);

        static::assertStringContainsString('No migrations have been executed yet.', $tester->getDisplay());
    }

    public function test_shows_latest_version(): void
    {
        $store = new FakeMigrationStore();
        $store->initialize();
        $store->complete(Version::fromString('20260401120000'), 10);
        $store->complete(Version::fromString('20260402100000'), 5);

        $tester = new CommandTester(new CurrentCommand($store));
        $tester->execute([]);

        static::assertStringContainsString('20260402100000', $tester->getDisplay());
    }
}
