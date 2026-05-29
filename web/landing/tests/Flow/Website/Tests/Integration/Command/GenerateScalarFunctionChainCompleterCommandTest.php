<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

use function file_get_contents;
use function Flow\Types\DSL\type_string;

final class GenerateScalarFunctionChainCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully(): void
    {
        $commandTester = $this->executeCommand('app:generate:scalar-function-chain-completer');

        static::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        static::assertStringContainsString('Generated ScalarFunctionChain completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_core_chain_methods(): void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('scalarfunctionchain.js')));

        $coreMethods = ['equals', 'isNull', 'isNotNull', 'cast', 'trim', 'lower', 'upper'];

        foreach ($coreMethods as $method) {
            static::assertStringContainsString(
                'label: "' . $method . '"',
                $content,
                "Missing core ScalarFunctionChain method: {$method}",
            );
        }
    }

    public function test_generated_js_contains_required_structure(): void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('scalarfunctionchain.js')));

        static::assertStringContainsString('CodeMirror Completer', $content);
        static::assertStringContainsString('scalarFunctionChainMethods', $content);
        static::assertStringContainsString('import { CompletionContext, snippet }', $content);
        static::assertStringContainsString('export function', $content);
    }

    public function test_generated_js_has_valid_completion_structure(): void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = type_string()->assert(file_get_contents($this->getOutputPath('scalarfunctionchain.js')));

        static::assertMatchesRegularExpression(
            '/label:\s*"[a-zA-Z]+"/i',
            $content,
            'Completions should have label property',
        );
        static::assertMatchesRegularExpression('/type:\s*"method"/i', $content, 'Completions should have type: method');
        static::assertStringContainsString(
            'ScalarFunctionChain"',
            $content,
            'Completions should reference ScalarFunctionChain class',
        );
    }
}
