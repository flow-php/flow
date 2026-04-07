<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Command\CatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class CatCommandTest extends TestCase
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

    public function test_fails_when_file_missing() : void
    {
        $tester = new CommandTester(new CatCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => 'memory://nope.txt']);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('File not found', $tester->getDisplay());
    }

    public function test_streams_file_to_stdout() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://hello.txt', 'hello world');

        $tester = new CommandTester(new CatCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://hello.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('hello world', $tester->getDisplay());
    }

    public function test_treats_path_without_protocol_as_local_file() : void
    {
        $dir = $this->context->tempDir();
        \file_put_contents($dir . '/payload.txt', 'local-bytes');

        $tester = new CommandTester(new CatCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => $dir . '/payload.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('local-bytes', $tester->getDisplay());
    }
}
