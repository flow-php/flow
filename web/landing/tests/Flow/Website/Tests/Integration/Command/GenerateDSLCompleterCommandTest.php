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

    public function test_generated_js_contains_core_dsl_functions() : void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = \file_get_contents($this->getOutputPath('dsl.js'));

        $coreFunctions = ['data_frame', 'from_array', 'to_output', 'ref', 'lit', 'collect'];

        foreach ($coreFunctions as $function) {
            self::assertStringContainsString('label: "' . $function . '"', $content, "Missing core DSL function: {$function}");
        }
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

    public function test_generated_js_has_valid_completion_structure() : void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = \file_get_contents($this->getOutputPath('dsl.js'));

        self::assertMatchesRegularExpression('/label:\s*"[a-z_]+"/i', $content, 'Completions should have label property');
        self::assertMatchesRegularExpression('/type:\s*"function"/i', $content, 'Completions should have type property');
        self::assertStringContainsString('detail: "flow\\u002D', $content, 'Completions should have detail property with flow- prefix');
        self::assertMatchesRegularExpression('/apply:\s*snippet\(/i', $content, 'Completions should use snippet() for apply');
    }
}
