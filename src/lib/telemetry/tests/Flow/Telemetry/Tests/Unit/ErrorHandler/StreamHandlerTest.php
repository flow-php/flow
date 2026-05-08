<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\ErrorHandler;

use Flow\Telemetry\ErrorHandler\StreamHandler;
use PHPUnit\Framework\TestCase;

final class StreamHandlerTest extends TestCase
{
    private string $tmpDir = '';

    protected function setUp() : void
    {
        $this->tmpDir = \sys_get_temp_dir() . '/flow-telemetry-stream-handler-' . \bin2hex(\random_bytes(6));
        \mkdir($this->tmpDir, 0755, true);
    }

    protected function tearDown() : void
    {
        if (!\is_dir($this->tmpDir)) {
            return;
        }

        $files = \glob($this->tmpDir . '/*');

        if ($files === false) {
            return;
        }

        foreach ($files as $file) {
            if (\is_file($file)) {
                \unlink($file);
            }
        }

        @\rmdir($this->tmpDir);
    }

    public function test_appends_formatted_throwable_with_newline_to_destination() : void
    {
        $destination = $this->tmpDir . '/errors.log';
        $handler = new StreamHandler($destination);

        $handler->handle(new \RuntimeException('boom'));

        $contents = (string) \file_get_contents($destination);

        self::assertStringContainsString('[flow-telemetry]', $contents);
        self::assertStringContainsString('RuntimeException', $contents);
        self::assertStringContainsString('boom', $contents);
        self::assertStringEndsWith("\n", $contents);
    }

    public function test_appends_one_line_per_handle_call() : void
    {
        $destination = $this->tmpDir . '/errors.log';
        $handler = new StreamHandler($destination);

        $handler->handle(new \RuntimeException('first'));
        $handler->handle(new \RuntimeException('second'));

        $contents = (string) \file_get_contents($destination);
        $lines = \array_values(\array_filter(\explode("\n", $contents), static fn (string $line) : bool => $line !== ''));

        self::assertCount(2, $lines);
        self::assertStringContainsString('first', $lines[0]);
        self::assertStringContainsString('second', $lines[1]);
    }

    public function test_creates_parent_directories_when_enabled() : void
    {
        $destination = $this->tmpDir . '/nested/dir/errors.log';
        $handler = new StreamHandler($destination, createDirectories: true);

        $handler->handle(new \RuntimeException('boom'));

        self::assertFileExists($destination);
    }

    public function test_supports_php_stream_wrappers() : void
    {
        $this->expectNotToPerformAssertions();

        $handler = new StreamHandler('php://memory');

        $handler->handle(new \RuntimeException('boom'));
    }

    public function test_swallows_failures_when_destination_is_unwritable() : void
    {
        $this->expectNotToPerformAssertions();

        $handler = new StreamHandler('/nonexistent-root-only-dir/errors.log', createDirectories: false);

        $handler->handle(new \RuntimeException('boom'));
    }

    public function test_throws_on_empty_destination() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        new StreamHandler('');
    }

    public function test_throws_on_invalid_file_permissions() : void
    {
        $this->expectException(\InvalidArgumentException::class);

        new StreamHandler($this->tmpDir . '/errors.log', filePermissions: 01000);
    }

    public function test_uses_a_custom_message_prefix() : void
    {
        $destination = $this->tmpDir . '/errors.log';
        $handler = new StreamHandler($destination, messagePrefix: '[custom]');

        $handler->handle(new \RuntimeException('boom'));

        self::assertStringContainsString('[custom]', (string) \file_get_contents($destination));
    }
}
