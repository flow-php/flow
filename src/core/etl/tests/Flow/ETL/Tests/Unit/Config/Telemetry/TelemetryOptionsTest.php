<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use PHPUnit\Framework\TestCase;

final class TelemetryOptionsTest extends TestCase
{
    public function test_builder_methods_preserve_other_flags(): void
    {
        $filesystemOptions = new FilesystemTelemetryOptions(traceStreams: true, collectMetrics: true);
        $options = new TelemetryOptions(
            traceLoading: true,
            traceTransformations: true,
            collectMetrics: true,
            filesystem: $filesystemOptions,
        );

        $newOptions = $options->traceLoading(false);

        static::assertFalse($newOptions->traceLoading);
        static::assertTrue($newOptions->traceTransformations);
        static::assertTrue($newOptions->collectMetrics);
        static::assertSame($filesystemOptions, $newOptions->filesystem);
    }

    public function test_collect_metrics_returns_new_instance_with_flag_enabled(): void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->collectMetrics();

        static::assertNotSame($options, $newOptions);
        static::assertTrue($newOptions->collectMetrics);
        static::assertFalse($options->collectMetrics);
    }

    public function test_default_options_have_all_flags_disabled(): void
    {
        $options = new TelemetryOptions();

        static::assertFalse($options->traceLoading);
        static::assertFalse($options->traceTransformations);
        static::assertFalse($options->collectMetrics);
        static::assertInstanceOf(FilesystemTelemetryOptions::class, $options->filesystem);
    }

    public function test_filesystem_returns_new_instance_with_options(): void
    {
        $options = new TelemetryOptions();
        $filesystemOptions = new FilesystemTelemetryOptions(traceStreams: true, collectMetrics: true);

        $newOptions = $options->filesystem($filesystemOptions);

        static::assertNotSame($options, $newOptions);
        static::assertSame($filesystemOptions, $newOptions->filesystem);
    }

    public function test_trace_loading_returns_new_instance_with_flag_enabled(): void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->traceLoading();

        static::assertNotSame($options, $newOptions);
        static::assertTrue($newOptions->traceLoading);
        static::assertFalse($options->traceLoading);
    }

    public function test_trace_transformations_returns_new_instance_with_flag_enabled(): void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->traceTransformations();

        static::assertNotSame($options, $newOptions);
        static::assertTrue($newOptions->traceTransformations);
        static::assertFalse($options->traceTransformations);
    }
}
