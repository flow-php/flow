<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use PHPUnit\Framework\TestCase;

final class ErrorLogHandlerTest extends TestCase
{
    private string $logFile = '';

    private string $previousErrorLog = '';

    protected function setUp() : void
    {
        $logFile = \tempnam(\sys_get_temp_dir(), 'flow-telemetry-error-log-');

        if ($logFile === false) {
            self::fail('Could not create temp log file');
        }

        $this->logFile = $logFile;
        $this->previousErrorLog = (string) \ini_get('error_log');
        \ini_set('error_log', $this->logFile);
    }

    protected function tearDown() : void
    {
        \ini_set('error_log', $this->previousErrorLog);

        if (\is_file($this->logFile)) {
            \unlink($this->logFile);
        }
    }

    public function test_collapses_newlines_when_expand_newlines_is_false() : void
    {
        $handler = new ErrorLogHandler();

        $handler->handle(new \RuntimeException("line1\nline2"));

        $contents = (string) \file_get_contents($this->logFile);
        $lines = \array_values(\array_filter(\explode("\n", $contents), static fn (string $line) : bool => $line !== ''));

        self::assertCount(1, $lines);
        self::assertStringContainsString('line1 line2', $lines[0]);
    }

    public function test_does_not_throw_when_invoked() : void
    {
        $this->expectNotToPerformAssertions();

        $handler = new ErrorLogHandler();

        $handler->handle(new \RuntimeException('boom'));
    }

    public function test_emits_a_single_line_with_default_settings() : void
    {
        $handler = new ErrorLogHandler();

        $handler->handle(new \RuntimeException('boom'));

        $contents = (string) \file_get_contents($this->logFile);

        self::assertStringContainsString('[flow-telemetry]', $contents);
        self::assertStringContainsString('RuntimeException', $contents);
        self::assertStringContainsString('boom', $contents);
    }

    public function test_emits_separate_lines_when_expand_newlines_is_true() : void
    {
        $handler = new ErrorLogHandler(expandNewlines: true);

        $handler->handle(new \RuntimeException("line1\nline2"));

        $contents = (string) \file_get_contents($this->logFile);
        $lines = \array_values(\array_filter(\explode("\n", $contents), static fn (string $line) : bool => $line !== ''));

        self::assertGreaterThanOrEqual(2, \count($lines));
    }

    public function test_uses_a_custom_message_prefix() : void
    {
        $handler = new ErrorLogHandler(messagePrefix: '[custom-prefix]');

        $handler->handle(new \RuntimeException('boom'));

        $contents = (string) \file_get_contents($this->logFile);

        self::assertStringContainsString('[custom-prefix]', $contents);
        self::assertStringNotContainsString('[flow-telemetry]', $contents);
    }
}
