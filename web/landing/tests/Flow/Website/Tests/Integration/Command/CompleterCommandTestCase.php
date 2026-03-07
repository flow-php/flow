<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Flow\Website\Kernel;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Console\Tester\CommandTester;

abstract class CompleterCommandTestCase extends KernelTestCase
{
    protected Application $application;

    protected function setUp() : void
    {
        parent::setUp();

        self::bootKernel();
        $this->application = new Application(self::$kernel);
    }

    protected function executeCommand(string $commandName) : CommandTester
    {
        $command = $this->application->find($commandName);
        $commandTester = new CommandTester($command);
        $commandTester->execute([]);

        return $commandTester;
    }

    protected function getOutputPath(string $filename) : string
    {
        return self::$kernel->getProjectDir() . '/assets/codemirror/completions/' . $filename;
    }

    #[\Override]
    protected static function getKernelClass() : string
    {
        return Kernel::class;
    }
}
