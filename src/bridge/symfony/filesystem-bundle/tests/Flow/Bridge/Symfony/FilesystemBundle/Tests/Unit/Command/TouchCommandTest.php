<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\FilesystemBundle\Tests\Unit\Command;

use function Flow\Filesystem\DSL\path;
use Flow\Bridge\Symfony\FilesystemBundle\Command\TouchCommand;
use Flow\Bridge\Symfony\FilesystemBundle\Tests\Context\CliCommandContext;
use PHPUnit\Framework\TestCase;
use Symfony\Component\Console\Command\Command;
use Symfony\Component\Console\Tester\CommandTester;

final class TouchCommandTest extends TestCase
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

    public function test_creates_empty_file() : void
    {
        $table = $this->context->defaultTable();
        $tester = new CommandTester(new TouchCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://new.txt']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertNotNull($table->for(path('memory://new.txt'))->status(path('memory://new.txt')));
    }

    public function test_force_overwrites_to_empty() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://exists.txt', 'not-empty');

        $tester = new CommandTester(new TouchCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://exists.txt', '--force' => true]);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertSame('', $table->for(path('memory://exists.txt'))->readFrom(path('memory://exists.txt'))->content());
    }

    public function test_refuses_existing_without_force() : void
    {
        $table = $this->context->defaultTable();
        $this->context->seedMemoryFile($table, 'memory://exists.txt', 'content');

        $tester = new CommandTester(new TouchCommand($this->context->resolver($table)));
        $exit = $tester->execute(['path' => 'memory://exists.txt']);

        self::assertSame(Command::FAILURE, $exit);
        self::assertStringContainsString('already exists', $tester->getDisplay());
    }

    public function test_treats_path_without_protocol_as_local_file() : void
    {
        $dir = $this->context->tempDir();

        $tester = new CommandTester(new TouchCommand($this->context->resolver()));
        $exit = $tester->execute(['path' => $dir . '/marker']);

        self::assertSame(Command::SUCCESS, $exit);
        self::assertFileExists($dir . '/marker');
    }
}
