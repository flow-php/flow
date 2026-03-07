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

    public function test_generated_js_contains_core_dataframe_methods() : void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = \file_get_contents($this->getOutputPath('dataframe.js'));

        $coreMethods = ['write', 'collect', 'fetch', 'run', 'withEntry', 'select', 'drop', 'filter', 'limit'];

        foreach ($coreMethods as $method) {
            self::assertStringContainsString('label: "' . $method . '"', $content, "Missing core DataFrame method: {$method}");
        }
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

    public function test_generated_js_has_valid_completion_structure() : void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = \file_get_contents($this->getOutputPath('dataframe.js'));

        self::assertMatchesRegularExpression('/label:\s*"[a-zA-Z]+"/i', $content, 'Completions should have label property');
        self::assertMatchesRegularExpression('/type:\s*"method"/i', $content, 'Completions should have type: method');
        self::assertStringContainsString('ETL', $content);
        self::assertStringContainsString('DataFrame"', $content, 'Completions should reference DataFrame class');
    }
}
