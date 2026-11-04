<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Flow\Website\Kernel;
use Override;
use Symfony\Bundle\FrameworkBundle\Console\Application;
use Symfony\Bundle\FrameworkBundle\Test\KernelTestCase;
use Symfony\Component\Console\Tester\CommandTester;

use function bin2hex;
use function file_get_contents;
use function Flow\Filesystem\DSL\native_local_filesystem;
use function Flow\Filesystem\DSL\path;
use function Flow\Types\DSL\structure_element;
use function Flow\Types\DSL\type_list;
use function Flow\Types\DSL\type_map;
use function Flow\Types\DSL\type_mixed;
use function Flow\Types\DSL\type_string;
use function Flow\Types\DSL\type_structure;
use function json_decode;
use function preg_match;
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

    /**
     * @return array<string, array{api?: string, documentation?: string, doc?: string, supertypes: list<string>, methods: list<array<string, mixed>>, staticMethods: list<array<string, mixed>>, constants: list<string>}>
     */
    protected function exportedClasses(): array
    {
        return type_map(type_string(), type_structure([
            'api' => structure_element('api', type_string(), optional: true),
            'documentation' => structure_element('documentation', type_string(), optional: true),
            'doc' => structure_element('doc', type_string(), optional: true),
            'supertypes' => type_list(type_string()),
            'methods' => type_list(type_map(type_string(), type_mixed())),
            'staticMethods' => type_list(type_map(type_string(), type_mixed())),
            'constants' => type_list(type_string()),
        ]))->assert($this->exportedJson('classes'));
    }

    /**
     * @return list<array<string, mixed>>
     */
    protected function exportedFunctions(): array
    {
        return type_list(type_map(type_string(), type_mixed()))->assert($this->exportedJson('functions'));
    }

    protected function exportedJson(string $constant): mixed
    {
        $matches = [];
        static::assertSame(1, preg_match(
            '/^export const ' . $constant . ' = (.+)$/m',
            type_string()->assert(file_get_contents($this->getOutputPath('flow-api.js'))),
            $matches,
        ));

        return json_decode($matches[1], true, 512, JSON_THROW_ON_ERROR);
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
