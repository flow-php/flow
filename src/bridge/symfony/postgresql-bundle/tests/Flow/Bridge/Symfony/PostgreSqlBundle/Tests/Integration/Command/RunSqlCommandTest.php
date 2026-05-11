<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class RunSqlCommandTest extends CommandTestCase
{
    public function test_runs_ddl_and_verifies_table_created(): void
    {
        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.sql_run');
        $tester = new CommandTester($command);
        $tester->execute(['sql' => 'CREATE TABLE flow_sql_test (id SERIAL PRIMARY KEY, name TEXT NOT NULL)']);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertTrue($this->context->tableExists('flow_sql_test'));
    }

    public function test_runs_select_query_and_returns_data(): void
    {
        /** @var Command $command */
        $command = $this->context->container()->get('flow.postgresql.command.sql_run');
        $tester = new CommandTester($command);
        $tester->execute(['sql' => "SELECT 1 AS result, 'hello' AS greeting"]);

        static::assertSame(Command::SUCCESS, $tester->getStatusCode());
        static::assertStringContainsString('result', $tester->getDisplay());
        static::assertStringContainsString('greeting', $tester->getDisplay());
        static::assertStringContainsString('hello', $tester->getDisplay());
        static::assertStringContainsString('1 row(s) returned', $tester->getDisplay());
    }
}
