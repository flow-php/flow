<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

use function file_get_contents;

final class GenerateDataFrameCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully(): void
    {
        $commandTester = $this->executeCommand('app:generate:data-frame-completer');

        static::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        static::assertStringContainsString('Generated DataFrame completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_core_dataframe_methods(): void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = file_get_contents($this->getOutputPath('dataframe.js'));

        $coreMethods = ['write', 'collect', 'fetch', 'run', 'withEntry', 'select', 'drop', 'filter', 'limit'];

        foreach ($coreMethods as $method) {
            static::assertStringContainsString(
                'label: "' . $method . '"',
                $content,
                "Missing core DataFrame method: {$method}",
            );
        }
    }

    public function test_generated_js_contains_required_structure(): void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = file_get_contents($this->getOutputPath('dataframe.js'));

        static::assertStringContainsString('CodeMirror Completer', $content);
        static::assertStringContainsString('dataframeMethods', $content);
        static::assertStringContainsString('import { CompletionContext, snippet }', $content);
        static::assertStringContainsString('export function', $content);
    }

    public function test_generated_js_has_valid_completion_structure(): void
    {
        $this->executeCommand('app:generate:data-frame-completer');

        $content = file_get_contents($this->getOutputPath('dataframe.js'));

        static::assertMatchesRegularExpression(
            '/label:\s*"[a-zA-Z]+"/i',
            $content,
            'Completions should have label property',
        );
        static::assertMatchesRegularExpression('/type:\s*"method"/i', $content, 'Completions should have type: method');
        static::assertStringContainsString('ETL', $content);
        static::assertStringContainsString('DataFrame"', $content, 'Completions should reference DataFrame class');
    }
}
