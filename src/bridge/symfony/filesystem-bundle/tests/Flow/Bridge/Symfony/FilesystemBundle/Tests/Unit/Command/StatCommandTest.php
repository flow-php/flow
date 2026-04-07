<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Command\StatCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class StatCommandTest extends TestCase
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

    public function test_fails_on_invalid_format() : void
    {
        $tester = new CommandTester(new StatCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => 'memory://x', '--format' => 'xml']);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('Unsupported --format', $tester->getDisplay());
    }

    public function test_fails_on_missing_path() : void
    {
        $tester = new CommandTester(new StatCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => 'memory://absent.txt']);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('Path not found', $tester->getDisplay());
    }

    public function test_human_output_shows_metadata() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://file.txt', 'hello');

        $tester = new CommandTester(new StatCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://file.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        $display = $tester->getDisplay();
        self::assertStringContainsString('memory://file.txt', $display);
        self::assertStringContainsString('file', $display);
        self::assertStringContainsString('memory', $display);
    }

    public function test_json_output_schema() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://f.bin', 'abcd');

        $tester = new CommandTester(new StatCommand($this->context->resolver($table)));
        $tester->execute(['path' => 'memory://f.bin', '--format' => 'json']);

        /** @var array{uri: string, protocol: string, path: string, type: string, size: null|int} $data */
        $data = \json_decode(\trim($tester->getDisplay()), true, flags: \JSON_THROW_ON_ERROR);

        self::assertSame('memory://f.bin', $data['uri']);
        self::assertSame('memory', $data['protocol']);
        self::assertSame('file', $data['type']);
        self::assertSame(4, $data['size']);
    }

    public function test_treats_path_without_protocol_as_local_file() : void
    {
        $dir = $this->context->tempDir();
        \file_put_contents($dir . '/local.txt', 'hi');

        $tester = new CommandTester(new StatCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => $dir . '/local.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertStringContainsString('file', $tester->getDisplay());
    }
}
