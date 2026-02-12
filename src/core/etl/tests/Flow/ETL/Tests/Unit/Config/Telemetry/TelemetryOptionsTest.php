<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use Flow\ETL\Config\Telemetry\TelemetryOptions;
use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;
use PHPUnit\Framework\TestCase;

final class TelemetryOptionsTest extends TestCase
{
    public function test_builder_methods_preserve_other_flags() : void
    {
        $filesystemOptions = new FilesystemTelemetryOptions(traceStreams: true, collectMetrics: true);
        $options = new TelemetryOptions(
            traceLoading: true,
            traceTransformations: true,
            collectMetrics: true,
            filesystem: $filesystemOptions,
        );

        $newOptions = $options->traceLoading(false);

        self::assertFalse($newOptions->traceLoading);
        self::assertTrue($newOptions->traceTransformations);
        self::assertTrue($newOptions->collectMetrics);
        self::assertSame($filesystemOptions, $newOptions->filesystem);
    }

    public function test_collect_metrics_returns_new_instance_with_flag_enabled() : void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->collectMetrics();

        self::assertNotSame($options, $newOptions);
        self::assertTrue($newOptions->collectMetrics);
        self::assertFalse($options->collectMetrics);
    }

    public function test_default_options_have_all_flags_disabled() : void
    {
        $options = new TelemetryOptions();

        self::assertFalse($options->traceLoading);
        self::assertFalse($options->traceTransformations);
        self::assertFalse($options->collectMetrics);
        self::assertInstanceOf(FilesystemTelemetryOptions::class, $options->filesystem);
    }

    public function test_filesystem_returns_new_instance_with_options() : void
    {
        $options = new TelemetryOptions();
        $filesystemOptions = new FilesystemTelemetryOptions(traceStreams: true, collectMetrics: true);

        $newOptions = $options->filesystem($filesystemOptions);

        self::assertNotSame($options, $newOptions);
        self::assertSame($filesystemOptions, $newOptions->filesystem);
    }

    public function test_trace_loading_returns_new_instance_with_flag_enabled() : void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->traceLoading();

        self::assertNotSame($options, $newOptions);
        self::assertTrue($newOptions->traceLoading);
        self::assertFalse($options->traceLoading);
    }

    public function test_trace_transformations_returns_new_instance_with_flag_enabled() : void
    {
        $options = new TelemetryOptions();

        $newOptions = $options->traceTransformations();

        self::assertNotSame($options, $newOptions);
        self::assertTrue($newOptions->traceTransformations);
        self::assertFalse($options->traceTransformations);
    }
}
