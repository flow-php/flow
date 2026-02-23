<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

final class GenerateFlowCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully() : void
    {
        $commandTester = $this->executeCommand('app:generate:flow-completer');

        self::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        self::assertStringContainsString('Generated Flow completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_required_structure() : void
    {
        $this->executeCommand('app:generate:flow-completer');

        $content = \file_get_contents($this->getOutputPath('flow.js'));

        self::assertStringContainsString('CodeMirror Completer', $content);
        self::assertStringContainsString('flowMethods', $content);
        self::assertStringContainsString('import { CompletionContext, snippet }', $content);
        self::assertStringContainsString('export function', $content);
    }

    public function test_generated_output_matches_expected_fixture() : void
    {
        $this->executeCommand('app:generate:flow-completer');

        $this->assertGeneratedFileMatchesFixture(
            $this->getOutputPath('flow.js'),
            $this->getFixturePath('flow.js')
        );
    }
}
