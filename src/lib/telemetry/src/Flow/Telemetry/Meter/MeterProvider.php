<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\{Attributes, InstrumentationScope, Resource};
use Flow\Telemetry\Meter\Exemplar\{ExemplarFilter, TraceBasedExemplarFilter};
use Flow\Telemetry\Provider\Clock\SystemClock;
use Psr\Clock\ClockInterface;

/**
 * Entry point for the metrics API.
 *
 * Creates and manages meters for different instrumentation scopes.
 * The behavior (void, memory, OTLP, etc.) is determined by the injected processor.
 *
 * Example usage:
 * ```php
 * // For testing with memory storage
 * $processor = new MemoryProcessor($exporter, $exporter, $exporter);
 * $provider = new MeterProvider($processor, new SystemClock());
 *
 * // For OTLP export
 * $processor = batching_metric_processor(otlp_metric_exporter($transport));
 * $provider = new MeterProvider($processor, new SystemClock());
 *
 * // For void/disabled metrics
 * $provider = new MeterProvider(new VoidProcessor(), new SystemClock());
 *
 * $meter = $provider->meter('my-service', '1.0.0');
 * $counter = $meter->createCounter('requests.total');
 * $counter->add(1);
 * ```
 */
final readonly class MeterProvider
{
    public function __construct(
        private MetricProcessor $processor,
        private ClockInterface $clock = new SystemClock(),
        private AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
        private ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
        private MetricLimits $limits = new MetricLimits(),
    ) {
    }

    /**
     * Create a meter for the given instrumentation scope.
     *
     * Note: Instance caching is handled by the Telemetry class.
     * This method always creates a new Meter instance.
     *
     * @param resource $resource The resource context for all metrics from this meter
     * @param string $name The name of the instrumentation scope (e.g., library name)
     * @param string $version The version of the instrumentation scope
     * @param null|string $schemaUrl Schema URL for semantic conventions
     * @param null|Attributes $attributes Additional scope attributes
     */
    public function meter(Resource $resource, string $name, string $version = 'unknown', ?string $schemaUrl = null, ?Attributes $attributes = null) : Meter
    {
        return new Meter(
            $resource,
            new InstrumentationScope($name, $version, $schemaUrl, $attributes ?? new Attributes()),
            $this->processor,
            $this->clock,
            $this->temporality,
            $this->exemplarFilter,
            $this->limits,
        );
    }
}
