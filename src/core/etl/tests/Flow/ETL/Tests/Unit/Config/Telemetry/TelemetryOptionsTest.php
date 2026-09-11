<?php

declare(strict_types=1);

namespace Flow\ETL\Tests\Unit\Config\Telemetry;

use Flow\ETL\Config\Telemetry\TelemetryOptions;
use PHPUnit\Framework\TestCase;
use ReflectionClass;
use ReflectionParameter;

use function array_map;

final class TelemetryOptionsTest extends TestCase
{
    public function test_builder_methods_preserve_other_flags(): void
    {
        $options = new TelemetryOptions(traceLoading: true, traceTransformations: true, collectMetrics: true);

        $newOptions = $options->traceLoading(false);

        static::assertFalse($newOptions->traceLoading);
        static::assertTrue($newOptions->traceTransformations);
        static::assertTrue($newOptions->collectMetrics);
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
        static::assertFalse($options->traceCache);
    }

    public function test_telemetry_options_has_no_filesystem_member(): void
    {
        // kept inert, this option would be a knob that silently does nothing
        $reflection = new ReflectionClass(TelemetryOptions::class);

        static::assertFalse($reflection->hasProperty('filesystem'));
        static::assertFalse($reflection->hasMethod('filesystem'));

        $constructor = $reflection->getConstructor();

        static::assertNotNull($constructor);
        static::assertSame(
            ['traceLoading', 'traceTransformations', 'traceCache', 'collectMetrics'],
            array_map(static fn(ReflectionParameter $p): string => $p->getName(), $constructor->getParameters()),
        );
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
