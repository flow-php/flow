<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\Telemetry\Context\MemoryContextStorage;
use Flow\Telemetry\Logger\LoggerProvider;
use Flow\Telemetry\Meter\MeterProvider;
use Flow\Telemetry\Provider\Void\{VoidLogProcessor, VoidMetricProcessor, VoidSpanProcessor};
use Flow\Telemetry\{Resource, Telemetry};
use Flow\Telemetry\Tracer\TracerProvider;
use Psr\Clock\ClockInterface;

/**
 * Immutable telemetry configuration for DataFrame operations.
 *
 * This class holds the telemetry instance and options that control
 * what telemetry data is captured during pipeline execution.
 */
final readonly class TelemetryConfig
{
    public function __construct(
        public Telemetry $telemetry,
        public TelemetryOptions $options,
    ) {
    }

    /**
     * Create a default telemetry configuration with void exporters.
     *
     * This is used when no telemetry is explicitly configured.
     */
    public static function default(ClockInterface $clock) : self
    {
        $contextStorage = new MemoryContextStorage();

        return new self(
            new Telemetry(
                Resource::create([]),
                new TracerProvider(
                    new VoidSpanProcessor(),
                    $clock,
                    $contextStorage,
                ),
                new MeterProvider(
                    new VoidMetricProcessor(),
                    $clock,
                ),
                new LoggerProvider(
                    new VoidLogProcessor(),
                    $clock,
                    $contextStorage,
                ),
            ),
            new TelemetryOptions(),
        );
    }
}
