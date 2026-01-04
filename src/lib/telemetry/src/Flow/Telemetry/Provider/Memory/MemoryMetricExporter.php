<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Meter\{Metric, MetricExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Exporter that stores metrics in memory for direct access.
 *
 * Useful for testing and inspection where you need direct access
 * to exported data without serialization.
 */
final class MemoryMetricExporter implements MetricExporter
{
    /**
     * @var array<Metric>
     */
    private array $metrics = [];

    /**
     * @param array<Metric> $metrics
     */
    public function export(array $metrics) : bool
    {
        foreach ($metrics as $metric) {
            $this->metrics[] = $metric;
        }

        return true;
    }

    /**
     * Get all exported metrics.
     *
     * @return array<Metric>
     */
    public function metrics() : array
    {
        return $this->metrics;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->metrics = [];
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
