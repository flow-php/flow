<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Mother;

use Flow\Telemetry\Context\Context;
use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Void\VoidLogProcessor;
use Flow\Telemetry\Provider\Void\VoidMetricProcessor;
use Flow\Telemetry\Provider\Void\VoidSpanProcessor;
use Flow\Telemetry\Resource;
use Flow\Telemetry\Telemetry;
use Flow\Telemetry\Tracer\Sampler\AlwaysOnSampler;
use Flow\Telemetry\Tracer\Sampler\SuppressingSampler;
use Flow\Telemetry\Tracer\SpanProcessor;
use Flow\Telemetry\Tracer\TracerProvider;

final class TelemetryMother
{
    public static function withSpanProcessor(SpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }

    /**
     * A Telemetry whose tracer provider mirrors production wiring — the configured sampler composed with
     * SuppressingSampler — over a context that already carries the suppression key. Any span created through
     * it must be dropped, so instrumentation can be asserted to emit nothing while tracing is suppressed.
     */
    public static function suppressed(SpanProcessor $spanProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage(Context::root()->withSuppressedTracing());

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider($spanProcessor, $clock, $contextStorage, new SuppressingSampler(new AlwaysOnSampler())),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }

    public static function withMetricProcessor(MetricProcessor $metricProcessor): Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create(['service.name' => 'test']),
            new TracerProvider(new VoidSpanProcessor(), $clock, $contextStorage),
            new MeterProvider($metricProcessor, $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
