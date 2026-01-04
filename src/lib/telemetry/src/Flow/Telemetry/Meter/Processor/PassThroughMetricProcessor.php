<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricProcessor};

/**
 * Exports each metric immediately when processed.
 *
 * Unlike BatchingMetricProcessor, this processor exports metrics synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of metrics is more important than performance.
 *
 * Example usage:
 * ```php
 * $processor = new PassThroughMetricProcessor($metricExporter);
 * ```
 */
final readonly class PassThroughMetricProcessor implements MetricProcessor
{
    public function __construct(
        private MetricExporter $exporter,
    ) {
    }

    public function exporter() : MetricExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        return true;
    }

    public function process(Metric $metric) : void
    {
        $this->exporter->export([$metric]);
    }
}
