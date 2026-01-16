<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\Transport\Transport;

/**
 * Interface for exporting metrics to external systems.
 *
 * Exporters are responsible for transmitting metric data to backends
 * like Prometheus, OTLP collectors, or custom storage systems.
 *
 * Example implementation:
 * ```php
 * final class OTLPMetricExporter implements MetricExporter
 * {
 *     public function export(array $metrics): bool
 *     {
 *         // Convert to OTLP format and send to collector
 *         return $this->client->sendMetrics($metrics);
 *     }
 * }
 * ```
 */
interface MetricExporter
{
    /**
     * Export a batch of metrics.
     *
     * Each metric carries its own Resource and InstrumentationScope.
     *
     * @param array<Metric> $metrics The metrics to export
     *
     * @return bool True on success, false on failure
     */
    public function export(array $metrics) : bool;

    /**
     * Get the transports used by this exporter.
     *
     * @return array<Transport> Always returns at least one transport
     */
    public function transports() : array;
}
