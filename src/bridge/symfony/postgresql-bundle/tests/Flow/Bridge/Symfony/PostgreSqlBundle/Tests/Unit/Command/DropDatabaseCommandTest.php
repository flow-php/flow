<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\DropDatabaseCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\DependencyInjection\Container;

final class DropDatabaseCommandTest extends TestCase
{
    public function test_has_correct_command_name() : void
    {
        $command = new DropDatabaseCommand(new Container(), 'default');

        self::assertSame('flow:database:drop', $command->getName());
    }

    public function test_requires_force_flag() : void
    {
        $tester = new CommandTester(new DropDatabaseCommand(new Container(), 'default'));
        $tester->execute([]);

        self::assertSame(Command::FAILURE, $tester->getStatusCode());
        self::assertStringContainsString('Use --force to proceed', $tester->getDisplay());
    }
}
