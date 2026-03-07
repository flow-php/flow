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

    public function test_generated_js_contains_core_flow_methods() : void
    {
        $this->executeCommand('app:generate:flow-completer');

        $content = \file_get_contents($this->getOutputPath('flow.js'));

        $coreMethods = ['setUp', 'extract', 'from', 'process', 'read'];

        foreach ($coreMethods as $method) {
            self::assertStringContainsString('label: "' . $method . '"', $content, "Missing core Flow method: {$method}");
        }
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

    public function test_generated_js_has_valid_completion_structure() : void
    {
        $this->executeCommand('app:generate:flow-completer');

        $content = \file_get_contents($this->getOutputPath('flow.js'));

        self::assertMatchesRegularExpression('/label:\s*"[a-zA-Z]+"/i', $content, 'Completions should have label property');
        self::assertMatchesRegularExpression('/type:\s*"method"/i', $content, 'Completions should have type: method');
        self::assertMatchesRegularExpression('/apply:\s*snippet\(/i', $content, 'Completions should use snippet() for apply');
    }
}
