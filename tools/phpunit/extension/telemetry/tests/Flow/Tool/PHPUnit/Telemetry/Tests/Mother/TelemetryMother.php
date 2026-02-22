<?php

declare(strict_types=1);

namespace Flow\Tool\PHPUnit\Telemetry\Tests\Mother;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Clock\SystemClock;
use Flow\Telemetry\Provider\Memory\{MemoryLogProcessor, MemoryMetricProcessor, MemorySpanProcessor};
use Flow\Telemetry\Provider\Void\{VoidLogExporter, VoidMetricExporter, VoidSpanExporter};
use Flow\Telemetry\{Telemetry};
use Flow\Telemetry\Tests\Mother\ResourceMother;
use Flow\Telemetry\Tracer\TracerProvider;

final class TelemetryMother
{
    public static function create(?MemorySpanProcessor $spanProcessor = null) : Telemetry
    {
        $clock = new SystemClock();
        $contextStorage = new MemoryContextStorage();

        return new Telemetry(
            ResourceMother::default(),
            new TracerProvider(
                $spanProcessor ?? new MemorySpanProcessor(new VoidSpanExporter()),
                $clock,
                $contextStorage,
            ),
            new MeterProvider(
                new MemoryMetricProcessor(new VoidMetricExporter()),
                $clock,
            ),
            new LoggerProvider(
                new MemoryLogProcessor(new VoidLogExporter()),
                $clock,
                $contextStorage,
            ),
        );
    }

    public static function withSpanProcessor(MemorySpanProcessor $spanProcessor) : Telemetry
    {
        return self::create($spanProcessor);
    }
}
