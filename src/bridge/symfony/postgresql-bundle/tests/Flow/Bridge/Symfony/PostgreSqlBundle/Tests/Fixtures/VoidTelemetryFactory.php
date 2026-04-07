<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSqlBundle\Tests\Fixtures;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Void\{VoidLogProcessor, VoidMetricProcessor, VoidSpanProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\TracerProvider;

final class VoidTelemetryFactory
{
    public static function create() : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            Resource::create([]),
            new TracerProvider(new VoidSpanProcessor(), $clock, $contextStorage),
            new MeterProvider(new VoidMetricProcessor(), $clock),
            new LoggerProvider(new VoidLogProcessor(), $clock, $contextStorage),
        );
    }
}
