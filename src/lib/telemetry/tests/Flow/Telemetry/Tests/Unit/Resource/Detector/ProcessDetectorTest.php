<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Unit\Resource\Detector;

use Flow\Telemetry\Resource\Attribute\ProcessAttribute;
use Flow\Telemetry\Resource\Detector\ProcessDetector;
use PHPUnit\Framework\TestCase;

final class ProcessDetectorTest extends TestCase
{
    public function test_detect_returns_command_args_when_available(): void
    {
        global $argv;

        if (!isset($argv) || !\is_array($argv) || \count($argv) === 0) {
            static::markTestSkipped('No command line arguments available');
        }

        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::COMMAND_ARGS->value));

        $commandArgs = $resource->get(ProcessAttribute::COMMAND_ARGS->value);

        static::assertIsArray($commandArgs);
        static::assertSame($argv, $commandArgs);
    }

    public function test_detect_returns_executable_name(): void
    {
        $detector = new ProcessDetector();
        $resource = $detector->detect();

        static::assertTrue($resource->has(ProcessAttribute::EXECUTABLE_NAME->value));
        static::assertSame(\basename(PHP_BINARY), $resource->get(ProcessAttribute::EXECUTABLE_NAME->value));
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
        if (!\function_exists('posix_getuid')) {
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
        static::assertSame(\getmypid(), $pid);
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
