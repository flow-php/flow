<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use function Flow\Filesystem\DSL\filesystem_telemetry_options;
use PHPUnit\Framework\TestCase;

final class FilesystemTelemetryOptionsTest extends TestCase
{
    public function test_default_options_enable_all_tracing() : void
    {
        $options = filesystem_telemetry_options();

        self::assertTrue($options->traceFilesystemOperations);
        self::assertTrue($options->traceStreamOperations);
    }

    public function test_fluent_interface_allows_chaining() : void
    {
        $options = filesystem_telemetry_options()
            ->withFilesystemOperations(false)
            ->withStreamOperations(false);

        self::assertFalse($options->traceFilesystemOperations);
        self::assertFalse($options->traceStreamOperations);
    }

    public function test_options_can_be_created_with_custom_values() : void
    {
        $options = filesystem_telemetry_options(
            traceFilesystemOperations: false,
            traceStreamOperations: true,
        );

        self::assertFalse($options->traceFilesystemOperations);
        self::assertTrue($options->traceStreamOperations);
    }

    public function test_with_filesystem_operations_creates_new_instance() : void
    {
        $original = filesystem_telemetry_options();
        $modified = $original->withFilesystemOperations(false);

        self::assertTrue($original->traceFilesystemOperations);
        self::assertFalse($modified->traceFilesystemOperations);
        self::assertTrue($modified->traceStreamOperations);
    }

    public function test_with_stream_operations_creates_new_instance() : void
    {
        $original = filesystem_telemetry_options();
        $modified = $original->withStreamOperations(false);

        self::assertTrue($original->traceStreamOperations);
        self::assertFalse($modified->traceStreamOperations);
        self::assertTrue($modified->traceFilesystemOperations);
    }
}
