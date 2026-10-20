<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Sanitizer;

use Flow\Telemetry\Sanitizer\StackTrace;
use PHPUnit\Framework\TestCase;
use RuntimeException;

final class StackTraceTest extends TestCase
{
    public function test_frame_without_file_renders_internal_function(): void
    {
        try {
            array_map(static fn(string $argument): never => throw new RuntimeException('failure'), ['SECRET_ARGUMENT']);
        } catch (RuntimeException $exception) {
            static::assertStringStartsWith('#0 [internal function]: ', StackTrace::format($exception));
            static::assertStringContainsString("\n#1 " . __FILE__ . '(', StackTrace::format($exception));
            static::assertStringContainsString('): array_map()' . "\n", StackTrace::format($exception));
        }
    }

    public function test_layout_matches_get_trace_as_string_when_arguments_are_ignored(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '1');

        try {
            array_map(static fn(string $argument): never => throw new RuntimeException('failure'), ['SECRET_ARGUMENT']);
        } catch (RuntimeException $exception) {
            static::assertSame(
                // PHP renders the included path as an argument of include/require frames even when arguments are ignored
                preg_replace(
                    "~: ((?:include|require)(?:_once)?)\\('[^\\n]*'\\)\$~m",
                    ': $1()',
                    $exception->getTraceAsString(),
                ),
                StackTrace::format($exception),
            );
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }

    public function test_method_frames_render_class_and_call_type(): void
    {
        $thrower = new class {
            public function fail(string $argument): never
            {
                throw new RuntimeException('failure');
            }

            public static function failStatically(string $argument): never
            {
                throw new RuntimeException('failure');
            }
        };

        try {
            $thrower->fail('SECRET_ARGUMENT');
        } catch (RuntimeException $exception) {
            static::assertStringStartsWith('#0 ' . __FILE__ . '(', StackTrace::format($exception));
            static::assertStringContainsString(
                '): ' . $thrower::class . '->fail()' . "\n",
                StackTrace::format($exception),
            );
        }

        try {
            $thrower::failStatically('SECRET_ARGUMENT');
        } catch (RuntimeException $exception) {
            static::assertStringContainsString(
                '): ' . $thrower::class . '::failStatically()' . "\n",
                StackTrace::format($exception),
            );
        }
    }

    public function test_trace_ends_with_main_frame(): void
    {
        $exception = new RuntimeException('failure');

        static::assertStringEndsWith("\n#" . count($exception->getTrace()) . ' {main}', StackTrace::format($exception));
    }

    public function test_trace_has_no_argument_values_when_arguments_are_collected(): void
    {
        $previous = ini_set('zend.exception_ignore_args', '0');
        $line = __LINE__ + 3;

        try {
            (static fn(string $argument): never => throw new RuntimeException('failure'))('SECRET_ARGUMENT');
        } catch (RuntimeException $exception) {
            static::assertStringContainsString('SECRET_', $exception->getTraceAsString());
            static::assertStringNotContainsString('SECRET_', StackTrace::format($exception));
            static::assertStringStartsWith('#0 ' . __FILE__ . '(' . $line . '): ', StackTrace::format($exception));
            static::assertStringContainsString('}()' . "\n#1 ", StackTrace::format($exception));
        } finally {
            ini_set('zend.exception_ignore_args', (string) $previous);
        }
    }
}
