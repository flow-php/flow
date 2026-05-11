<?php

declare(strict_types=1);

namespace Flow\Filesystem\Tests\Unit\Telemetry;

use PHPUnit\Framework\TestCase;

use function Flow\Filesystem\DSL\filesystem_telemetry_options;

final class FilesystemTelemetryOptionsTest extends TestCase
{
    public function test_default_options_have_expected_values(): void
    {
        $options = filesystem_telemetry_options();

        static::assertTrue($options->traceStreams);
        static::assertTrue($options->collectMetrics);
    }

    public function test_fluent_interface_allows_chaining(): void
    {
        $options = filesystem_telemetry_options()->traceStreams(false)->collectMetrics(false);

        static::assertFalse($options->traceStreams);
        static::assertFalse($options->collectMetrics);
    }

    public function test_options_can_be_created_with_custom_values(): void
    {
        $options = filesystem_telemetry_options(traceStreams: false, collectMetrics: false);

        static::assertFalse($options->traceStreams);
        static::assertFalse($options->collectMetrics);
    }

    public function test_with_collect_metrics_creates_new_instance(): void
    {
        $original = filesystem_telemetry_options();
        $modified = $original->collectMetrics(false);

        static::assertTrue($original->collectMetrics);
        static::assertFalse($modified->collectMetrics);
    }

    public function test_with_trace_streams_creates_new_instance(): void
    {
        $original = filesystem_telemetry_options();
        $modified = $original->traceStreams(false);

        static::assertTrue($original->traceStreams);
        static::assertFalse($modified->traceStreams);
    }
}
