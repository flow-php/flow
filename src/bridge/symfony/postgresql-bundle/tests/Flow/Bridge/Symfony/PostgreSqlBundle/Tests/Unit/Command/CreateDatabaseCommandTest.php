<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\PostgreSqlBundle\Command\CreateDatabaseCommand;
use PHPUnit\Framework\TestCase;
use Symfony\Component\DependencyInjection\Container;

final class CreateDatabaseCommandTest extends TestCase
{
    public function test_has_correct_command_name() : void
    {
        $command = new CreateDatabaseCommand(new Container(), 'default');

        self::assertSame('flow:database:create', $command->getName());
    }
}
