<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

final class GenerateDSLCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully() : void
    {
        $commandTester = $this->executeCommand('app:generate:dsl-completer');

        self::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        self::assertStringContainsString('Generated DSL completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_required_structure() : void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = \file_get_contents($this->getOutputPath('dsl.js'));

        self::assertStringContainsString('CodeMirror Completer', $content);
        self::assertStringContainsString('dslFunctions', $content);
        self::assertStringContainsString('import { CompletionContext, snippet }', $content);
        self::assertStringContainsString('export function', $content);
    }

    public function test_generated_output_matches_expected_fixture() : void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $this->assertGeneratedFileMatchesFixture(
            $this->getOutputPath('dsl.js'),
            $this->getFixturePath('dsl.js')
        );
    }
}
