<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use Flow\Bridge\Symfony\FilesystemBundle\Command\LsCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class LsCommandTest extends TestCase
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
        $tester = new CommandTester(new LsCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => 'memory://d', '--format' => 'xml']);

        self::assertSame(Command::FAILURE, $exit);
    }

    public function test_json_output_emits_array() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://j/a.txt', 'A');

        $tester = new CommandTester(new LsCommand($this->context->resolver($table)));
        $tester->execute(['path' => 'memory://j', '--format' => 'json']);

        /** @var array<int, array{uri: string, type: string, size: null|int}> $data */
        $data = \json_decode(\trim($tester->getDisplay()), true, flags: \JSON_THROW_ON_ERROR);
        self::assertCount(1, $data);
        self::assertSame('memory://j/a.txt', $data[0]['uri']);
    }

    public function test_lists_directories_alongside_files() : void
    {
        $dir = $this->context->tempDir();
        \mkdir($dir . '/sub');
        \file_put_contents($dir . '/a.txt', 'A');

        $tester = new CommandTester(new LsCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => $dir, '--long' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        $display = $tester->getDisplay();
        self::assertStringContainsString('a.txt', $display);
        self::assertStringContainsString('sub', $display);
        self::assertStringContainsString('directory', $display);
    }

    public function test_lists_memory_directory_entries() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://dir/a.txt', 'A');
        $this->context->seedMemoryFile($table, 'memory://dir/b.txt', 'BB');

        $tester = new CommandTester(new LsCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://dir']);

        self::assertSame(Command::SUCCESS, $exit);
        $display = $tester->getDisplay();
        self::assertStringContainsString('memory://dir/a.txt', $display);
        self::assertStringContainsString('memory://dir/b.txt', $display);
    }

    public function test_long_output_includes_size_column() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://d/a.txt', 'hello');

        $tester = new CommandTester(new LsCommand($this->context->resolver($table)));
        $tester->execute(['path' => 'memory://d', '--long' => true]);

        $display = $tester->getDisplay();
        self::assertStringContainsString('Size', $display);
        self::assertStringContainsString('5', $display);
    }

    public function test_treats_path_without_protocol_as_local_directory() : void
    {
        $dir = $this->context->tempDir();
        \file_put_contents($dir . '/a.txt', 'A');

        $tester = new CommandTester(new LsCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => $dir]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertStringContainsString('a.txt', $tester->getDisplay());
    }
}
