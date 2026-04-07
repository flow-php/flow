<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\FilesystemBundle\Command\MvCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class MvCommandTest extends TestCase
{
    private CliCommandContext $context;

    protected function setUp() : void
    {
        $this->context = new CliCommandContext();
    }

    protected function tearDown() : void
    {
        $this->context->cleanup();
    }

    public function test_fails_when_source_missing() : void
    {
        $tester = new CommandTester(new MvCommand($this->context->resolver()));
        $exit = $tester->execute(['source' => 'memory://absent', 'destination' => 'memory://dest']);

        self::assertSame(Command::FAILURE, $exit);
    }

    public function test_moves_between_protocols_on_same_fstab() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://src.txt', 'payload');

        $dir = $this->context->tempDir();
        $destUri = 'file://' . $dir . '/dest.txt';

        $tester = new CommandTester(new MvCommand($this->context->resolver($table)));
        $exit = $tester->execute(['source' => 'memory://src.txt', 'destination' => $destUri]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('payload', \file_get_contents($dir . '/dest.txt'));
        self::assertNull($table->for(path('memory://src.txt'))->status(path('memory://src.txt')));
    }
}
