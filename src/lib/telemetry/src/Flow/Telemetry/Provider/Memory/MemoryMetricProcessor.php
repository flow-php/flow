<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricProcessor, MetricType};

/**
 * Processor that stores metrics in memory and exports via configured exporter.
 */
final class MemoryMetricProcessor implements MetricProcessor
{
    /**
     * @var array<Metric>
     */
    private array $metrics = [];

    public function __construct(
        private readonly MetricExporter $metricExporter,
    ) {
    }

    /**
     * Get the total number of recorded metrics.
     */
    public function countMetrics() : int
    {
        return \count($this->metrics);
    }

    public function exporter() : MetricExporter
    {
        return $this->metricExporter;
    }

    public function flush() : bool
    {
        if (\count($this->metrics) === 0) {
            return true;
        }

        return $this->metricExporter->export($this->metrics);
    }

    /**
     * Get all recorded metrics.
     *
     * @return array<Metric>
     */
    public function metrics() : array
    {
        return $this->metrics;
    }

    /**
     * Get all metrics of a specific type.
     *
     * @return array<Metric>
     */
    public function metricsOfType(MetricType $type) : array
    {
        return \array_values(\array_filter(
            $this->metrics,
            static fn (Metric $metric) : bool => $metric->type === $type,
        ));
    }

    /**
     * Get all metrics with a specific name.
     *
     * @return array<Metric>
     */
    public function metricsWithName(string $name) : array
    {
        return \array_values(\array_filter(
            $this->metrics,
            static fn (Metric $metric) : bool => $metric->name === $name,
        ));
    }

    public function process(Metric $metric) : void
    {
        $this->metrics[] = $metric;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->metrics = [];
    }
}
