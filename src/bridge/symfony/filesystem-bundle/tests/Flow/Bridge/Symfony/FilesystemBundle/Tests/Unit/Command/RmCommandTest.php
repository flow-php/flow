<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\FilesystemBundle\Command\RmCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class RmCommandTest extends TestCase
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

    public function test_fails_on_missing_path() : void
    {
        $tester = new CommandTester(new RmCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => 'memory://absent']);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('Path not found', $tester->getDisplay());
    }

    public function test_recursive_removes_directory() : void
    {
        $dir = $this->context->tempDir();
        \file_put_contents($dir . '/a.txt', 'hi');

        $table = $this->context->defaultTable();
        $tester = new CommandTester(new RmCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'file://' . $dir, '--recursive' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertFalse(\is_dir($dir));
    }

    public function test_removes_memory_file() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://to-delete.txt', 'x');

        $tester = new CommandTester(new RmCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://to-delete.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertNull($table->for(path('memory://to-delete.txt'))->status(path('memory://to-delete.txt')));
    }

    public function test_requires_recursive_for_directory() : void
    {
        $dir = $this->context->tempDir();
        \file_put_contents($dir . '/a.txt', 'hi');

        $table = $this->context->defaultTable();
        $tester = new CommandTester(new RmCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'file://' . $dir]);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('--recursive', $tester->getDisplay());
    }
}
