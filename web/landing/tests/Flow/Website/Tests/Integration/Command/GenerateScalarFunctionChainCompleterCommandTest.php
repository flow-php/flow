<?php

declare(strict_types=1);

namespace Flow\Website\Tests\Integration\Command;

use Symfony\Component\Console\Command\Command;

final class GenerateScalarFunctionChainCompleterCommandTest extends CompleterCommandTestCase
{
    public function test_command_executes_successfully() : void
    {
        $commandTester = $this->executeCommand('app:generate:scalar-function-chain-completer');

        self::assertSame(Command::SUCCESS, $commandTester->getStatusCode());
        self::assertStringContainsString('Generated ScalarFunctionChain completer', $commandTester->getDisplay());
    }

    public function test_generated_js_contains_core_chain_methods() : void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = \file_get_contents($this->getOutputPath('scalarfunctionchain.js'));

        $coreMethods = ['equals', 'isNull', 'isNotNull', 'cast', 'trim', 'lower', 'upper'];

        foreach ($coreMethods as $method) {
            self::assertStringContainsString('label: "' . $method . '"', $content, "Missing core ScalarFunctionChain method: {$method}");
        }
    }

    public function test_generated_js_contains_required_structure() : void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = \file_get_contents($this->getOutputPath('scalarfunctionchain.js'));

        self::assertStringContainsString('CodeMirror Completer', $content);
        self::assertStringContainsString('scalarFunctionChainMethods', $content);
        self::assertStringContainsString('import { CompletionContext, snippet }', $content);
        self::assertStringContainsString('export function', $content);
    }

    public function test_generated_js_has_valid_completion_structure() : void
    {
        $this->executeCommand('app:generate:scalar-function-chain-completer');

        $content = \file_get_contents($this->getOutputPath('scalarfunctionchain.js'));

        self::assertMatchesRegularExpression('/label:\s*"[a-zA-Z]+"/i', $content, 'Completions should have label property');
        self::assertMatchesRegularExpression('/type:\s*"method"/i', $content, 'Completions should have type: method');
        self::assertStringContainsString('ScalarFunctionChain"', $content, 'Completions should reference ScalarFunctionChain class');
    }
}
