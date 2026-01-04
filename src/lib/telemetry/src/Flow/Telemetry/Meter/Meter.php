<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\{InstrumentationScope, Resource};
use Flow\Telemetry\Meter\Exemplar\{ExemplarFilter, TraceBasedExemplarFilter};
use Flow\Telemetry\Meter\Instrument\{Counter, Gauge, Histogram, Instrument, UpDownCounter};
use Psr\Clock\ClockInterface;

/**
 * Meter for creating metric instruments.
 *
 * A Meter creates instruments for a specific instrumentation scope.
 * Instruments are cached and returned by name - calling createCounter()
 * twice with the same name returns the same Counter instance.
 *
 * Example usage:
 * ```php
 * $meter = $meterProvider->meter('my-service', '1.0.0');
 *
 * $counter = $meter->createCounter('requests.total', 'requests', 'Total requests');
 * $counter->add(1, ['http.method' => 'GET']);
 *
 * $histogram = $meter->createHistogram('request.duration', 'ms', 'Request duration');
 * $histogram->record(42.5, ['http.status' => 200]);
 * ```
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/#meter
 */
final class Meter
{
    /**
     * Cached instruments by key (name:type).
     *
     * @var array<string, Instrument>
     */
    private array $instruments = [];

    /**
     * @param resource $resource The resource context for all metrics from this meter
     * @param InstrumentationScope $scope The instrumentation scope
     * @param MetricProcessor $processor The metric processor
     * @param ClockInterface $clock Clock for timestamps
     * @param AggregationTemporality $temporality Aggregation temporality for metrics
     * @param ExemplarFilter $exemplarFilter Filter for exemplar sampling
     */
    public function __construct(
        private readonly Resource $resource,
        private InstrumentationScope $scope,
        private readonly MetricProcessor $processor,
        private readonly ClockInterface $clock,
        private readonly AggregationTemporality $temporality = AggregationTemporality::CUMULATIVE,
        private readonly ExemplarFilter $exemplarFilter = new TraceBasedExemplarFilter(),
    ) {
    }

    /**
     * Collect all aggregated metrics from all instruments.
     *
     * @return array<Metric>
     */
    public function collect() : array
    {
        $metrics = [];

        foreach ($this->instruments as $instrument) {
            foreach ($instrument->collect() as $metric) {
                $metrics[] = $metric;
            }
        }

        return $metrics;
    }

    /**
     * Create or get a Counter instrument.
     *
     * Counters are monotonically increasing - they only go up.
     * Use for counting occurrences: requests, errors, items processed.
     *
     * @param string $name Metric name (e.g., 'http.requests', 'flow.rows.processed')
     * @param null|string $unit Unit of measurement (e.g., 'rows', 'bytes')
     * @param null|string $description Human-readable description
     */
    public function createCounter(
        string $name,
        ?string $unit = null,
        ?string $description = null,
    ) : Counter {
        $key = $name . ':counter';

        if (!array_key_exists($key, $this->instruments)) {
            $this->instruments[$key] = new Counter(
                $name,
                $this->resource,
                $this->scope,
                $this->clock,
                $this->temporality,
                $this->exemplarFilter,
                $unit,
                $description,
            );
        }

        /** @var Counter */
        return $this->instruments[$key];
    }

    /**
     * Create or get a Gauge instrument.
     *
     * Gauges record the current value at a point in time.
     * Use for: temperature, memory usage, CPU utilization.
     *
     * @param string $name Metric name (e.g., 'system.memory.usage', 'cpu.utilization')
     * @param null|string $unit Unit of measurement (e.g., 'bytes', '%')
     * @param null|string $description Human-readable description
     */
    public function createGauge(
        string $name,
        ?string $unit = null,
        ?string $description = null,
    ) : Gauge {
        $key = $name . ':gauge';

        if (!array_key_exists($key, $this->instruments)) {
            $this->instruments[$key] = new Gauge(
                $name,
                $this->resource,
                $this->scope,
                $this->clock,
                $this->exemplarFilter,
                $unit,
                $description,
            );
        }

        /** @var Gauge */
        return $this->instruments[$key];
    }

    /**
     * Create or get a Histogram instrument.
     *
     * Histograms track the statistical distribution of values.
     * Use for: request latency, response sizes, batch sizes.
     *
     * @param string $name Metric name (e.g., 'http.request.duration', 'flow.batch.size')
     * @param null|string $unit Unit of measurement (e.g., 'ms', 'bytes', 'rows')
     * @param null|string $description Human-readable description
     * @param null|array<float> $boundaries Explicit bucket boundaries (null uses default)
     */
    public function createHistogram(
        string $name,
        ?string $unit = null,
        ?string $description = null,
        ?array $boundaries = null,
    ) : Histogram {
        $key = $name . ':histogram';

        if (!array_key_exists($key, $this->instruments)) {
            $this->instruments[$key] = new Histogram(
                $name,
                $this->resource,
                $this->scope,
                $this->clock,
                $this->temporality,
                $this->exemplarFilter,
                $unit,
                $description,
                $boundaries ?? Histogram::DEFAULT_BOUNDARIES,
            );
        }

        /** @var Histogram */
        return $this->instruments[$key];
    }

    /**
     * Create or get an UpDownCounter instrument.
     *
     * UpDownCounters track values that can go both up and down.
     * Use for: active connections, queue size, items in cache.
     *
     * @param string $name Metric name (e.g., 'queue.size', 'active.connections')
     * @param null|string $unit Unit of measurement
     * @param null|string $description Human-readable description
     */
    public function createUpDownCounter(
        string $name,
        ?string $unit = null,
        ?string $description = null,
    ) : UpDownCounter {
        $key = $name . ':updowncounter';

        if (!array_key_exists($key, $this->instruments)) {
            $this->instruments[$key] = new UpDownCounter(
                $name,
                $this->resource,
                $this->scope,
                $this->clock,
                $this->temporality,
                $this->exemplarFilter,
                $unit,
                $description,
            );
        }

        /** @var UpDownCounter */
        return $this->instruments[$key];
    }

    /**
     * Get the instrumentation scope.
     */
    public function instrumentationScope() : InstrumentationScope
    {
        return $this->scope;
    }

    /**
     * Get the meter name.
     */
    public function name() : string
    {
        return $this->scope->name;
    }

    /**
     * Get the processor used by this meter.
     */
    public function processor() : MetricProcessor
    {
        return $this->processor;
    }

    /**
     * Get the meter version.
     */
    public function version() : string
    {
        return $this->scope->version;
    }

    /**
     * Change the instrumentation scope for this meter.
     *
     * This mutates the meter instance and returns it for method chaining.
     */
    public function withInstrumentationScope(InstrumentationScope $scope) : self
    {
        $this->scope = $scope;

        return $this;
    }
}
