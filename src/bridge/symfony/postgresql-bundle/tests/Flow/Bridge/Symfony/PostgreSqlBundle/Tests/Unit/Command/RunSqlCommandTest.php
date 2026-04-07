<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\RunSqlCommand;
use Flow\PostgreSql\Migrations\Tests\Double\SpyClient;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class RunSqlCommandTest extends TestCase
{
    public function test_execute_non_select_query() : void
    {
        $client = new SpyClient();

        $container = new Container();
        $container->set('flow.postgresql.default.client', $client);

        $tester = new CommandTester(new RunSqlCommand($container, 'default'));
        $tester->execute(['sql' => 'DELETE FROM users WHERE id = 1']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertStringContainsString('0 row(s) affected.', $tester->getDisplay());
        self::assertSame('DELETE FROM users WHERE id = 1', $client->executedQueries[0]['sql']);
    }

    public function test_uses_connection_option() : void
    {
        $client = new SpyClient();

        $container = new Container();
        $container->set('flow.postgresql.other.client', $client);

        $tester = new CommandTester(new RunSqlCommand($container, 'default'));
        $tester->execute(['sql' => 'UPDATE users SET active = true', '--connection' => 'other']);

        self::assertSame(Command::SUCCESS, $tester->getStatusCode());
        self::assertSame('UPDATE users SET active = true', $client->executedQueries[0]['sql']);
    }
}
