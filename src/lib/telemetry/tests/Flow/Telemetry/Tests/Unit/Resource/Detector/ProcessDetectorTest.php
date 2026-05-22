<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\ProcessAttribute;
use Flow\Telemetry\Resource\Detector\ProcessDetector;
use PHPUnit\Framework\TestCase;

use function basename;
use function function_exists;
use function getmypid;

final class ProcessDetectorTest extends TestCase
{
    public function test_detect_returns_command_args_when_available(): void
    {
        global $argv;

        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::COMMAND_ARGS->value));

        $commandArgs = $resource->get(ProcessAttribute::COMMAND_ARGS->value);

        static::assertIsArray($commandArgs);
        static::assertSame($argv, $commandArgs);
    }

    public function test_detect_handles_null_argv_without_crashing(): void
    {
        global $argv;

        $original = $argv;
        $argv = null;

        try {
            $detector = new ProcessDetector();
            $resource = $detector->detect();

            static::assertFalse(
                $resource->has(ProcessAttribute::COMMAND_ARGS->value),
                'COMMAND_ARGS must not be set when $argv is null',
            );
        } finally {
            $argv = $original;
        }
    }

    public function test_detect_command_falls_back_when_argv_is_null_and_script_filename_missing(): void
    {
        global $argv;

        $originalArgv = $argv;
        // @mago-ignore analysis:redundant-null-coalesce
        $originalScriptFilename = $_SERVER['SCRIPT_FILENAME'] ?? null;

        $argv = null;
        unset($_SERVER['SCRIPT_FILENAME']);

        try {
            $detector = new ProcessDetector();
            $resource = $detector->detect();

            static::assertTrue($resource->has(ProcessAttribute::COMMAND->value));
            static::assertSame('unknown', $resource->get(ProcessAttribute::COMMAND->value));
        } finally {
            $argv = $originalArgv;

            // @mago-ignore analysis:redundant-condition,redundant-comparison
            if ($originalScriptFilename !== null) {
                $_SERVER['SCRIPT_FILENAME'] = $originalScriptFilename;
            }
        }
    }

    public function test_detect_returns_executable_name(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::EXECUTABLE_NAME->value));
        static::assertSame(basename(PHP_BINARY), $resource->get(ProcessAttribute::EXECUTABLE_NAME->value));
    }

    public function test_detect_returns_executable_path(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::EXECUTABLE_PATH->value));
        static::assertSame(PHP_BINARY, $resource->get(ProcessAttribute::EXECUTABLE_PATH->value));
    }

    public function test_detect_returns_process_owner_on_posix_systems(): void
    {
        if (!function_exists('posix_getuid')) {
            static::markTestSkipped('POSIX functions not available');
        }

        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::OWNER->value));

        $owner = $resource->get(ProcessAttribute::OWNER->value);

        static::assertIsString($owner);
        static::assertNotEmpty($owner);
    }

    public function test_detect_returns_process_pid(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::PID->value));

        $pid = $resource->get(ProcessAttribute::PID->value);

        static::assertIsInt($pid);
        static::assertSame(getmypid(), $pid);
    }

    public function test_detect_returns_runtime_name(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::RUNTIME_NAME->value));
        static::assertSame('PHP', $resource->get(ProcessAttribute::RUNTIME_NAME->value));
    }

    public function test_detect_returns_runtime_version(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::RUNTIME_VERSION->value));
        static::assertSame(PHP_VERSION, $resource->get(ProcessAttribute::RUNTIME_VERSION->value));
    }

    public function test_detected_resource_has_minimum_required_attributes(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertFalse($resource->isEmpty());
        static::assertGreaterThanOrEqual(5, $resource->count());
    }
}
