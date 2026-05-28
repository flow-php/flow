<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Console\Tester\CommandTester;
use Symfony\Component\HttpKernel\KernelInterface;

use function Flow\Types\DSL\type_instance_of;

abstract class CompleterCommandTestCase extends KernelTestCase
{
    protected Application $application;

    protected function setUp(): void
    {
        parent::setUp();

        $this->application = new Application(self::bootKernel());
    }

    protected function executeCommand(string $commandName): CommandTester
    {
        $command = $this->application->find($commandName);
        $commandTester = new CommandTester($command);
        $commandTester->execute([]);

        return $commandTester;
    }

    protected function getOutputPath(string $filename): string
    {
        return (
            type_instance_of(KernelInterface::class)->assert(self::$kernel)->getProjectDir()
            . '/assets/codemirror/completions/'
            . $filename
        );
    }

    #[Override]
    protected static function getKernelClass(): string
    {
        return Kernel::class;
    }
}
