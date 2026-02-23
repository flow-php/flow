<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

final class GenerateDataFrameCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully() : void
    {
        $commandTester = $this->executeCommand('app:generate:data-frame-completer');

        self::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        self::assertStringContainsString('Generated DataFrame completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_required_structure() : void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = \file_get_contents($this->getOutputPath('dataframe.js'));

        self::assertStringContainsString('CodeMirror Completer', $content);
        self::assertStringContainsString('dataframeMethods', $content);
        self::assertStringContainsString('import { CompletionContext, snippet }', $content);
        self::assertStringContainsString('export function', $content);
    }

    public function test_generated_output_matches_expected_fixture() : void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $this->assertGeneratedFileMatchesFixture(
            $this->getOutputPath('dataframe.js'),
            $this->getFixturePath('dataframe.js')
        );
    }
}
