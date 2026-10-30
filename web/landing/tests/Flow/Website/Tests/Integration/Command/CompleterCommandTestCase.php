<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Console\Tester\CommandTester;

use function bin2hex;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function random_bytes;

abstract class CompleterCommandTestCase extends KernelTestCase
{
    protected Application $application;

    protected string $outputDir;

    protected function setUp(): void
    {
        parent::setUp();

        $kernel = self::bootKernel();
        $this->application = new Application($kernel);
        $this->outputDir = $kernel->getProjectDir() . '/var/completers-' . bin2hex(random_bytes(8));
    }

    #[Override]
    protected function tearDown(): void
    {
        native_local_filesystem()->rm(path($this->outputDir));

        parent::tearDown();
    }

    protected function executeCommand(string $commandName): CommandTester
    {
        $command = $this->application->find($commandName);
        $commandTester = new CommandTester($command);
        $commandTester->execute(['--output-dir' => $this->outputDir]);

        return $commandTester;
    }

    protected function getOutputPath(string $filename): string
    {
        return $this->outputDir . '/' . $filename;
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
