<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class GenerateDSLCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully(): void
    {
        $commandTester = $this->executeCommand('app:generate:dsl-completer');

        static::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        static::assertStringContainsString('Generated DSL completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_core_dsl_functions(): void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('dsl.js')));

        $coreFunctions = ['data_frame', 'from_array', 'to_output', 'ref', 'lit', 'collect'];

        foreach ($coreFunctions as $function) {
            static::assertStringContainsString(
                'label: "' . $function . '"',
                $content,
                "Missing core DSL function: {$function}",
            );
        }
    }

    public function test_generated_js_contains_required_structure(): void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('dsl.js')));

        static::assertStringContainsString('CodeMirror Completer', $content);
        static::assertStringContainsString('dslFunctions', $content);
        static::assertStringContainsString('import { CompletionContext, snippet }', $content);
        static::assertStringContainsString('export function', $content);
    }

    public function test_generated_js_has_valid_completion_structure(): void
    {
        $this->executeCommand('app:generate:dsl-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('dsl.js')));

        static::assertMatchesRegularExpression(
            '/label:\s*"[a-z_]+"/i',
            $content,
            'Completions should have label property',
        );
        static::assertMatchesRegularExpression(
            '/type:\s*"function"/i',
            $content,
            'Completions should have type property',
        );
        static::assertStringContainsString(
            'detail: "flow\\u002D',
            $content,
            'Completions should have detail property with flow- prefix',
        );
        static::assertMatchesRegularExpression(
            '/apply:\s*snippet\(/i',
            $content,
            'Completions should use snippet() for apply',
        );
    }
}
