<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Exporter;

use Flow\Telemetry\Meter\{Metric, MetricExporter};
use Flow\Telemetry\Transport\Transport;

/**
 * Exports metrics to OTLP endpoint.
 *
 * Example usage:
 * ```php
 * $exporter = new OTLPMetricExporter(
 *     transport: $httpTransport,
 * );
 *
 * $exporter->export($metrics);
 * ```
 */
final readonly class OTLPMetricExporter implements MetricExporter
{
    public function __construct(
        private Transport $transport,
    ) {
    }

    /**
     * @param array<Metric> $metrics
     */
    public function export(array $metrics) : bool
    {
        if (\count($metrics) === 0) {
            return true;
        }

        try {
            $this->transport->sendMetrics($metrics);

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [$this->transport];
    }
}
