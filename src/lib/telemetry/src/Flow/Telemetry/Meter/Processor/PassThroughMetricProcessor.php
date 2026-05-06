<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\{Metric, MetricProcessor};
use Flow\Telemetry\Signal\Signals;

/**
 * Exports each metric immediately when processed.
 *
 * Unlike BatchingMetricProcessor, this processor exports metrics synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of metrics is more important than performance.
 */
final readonly class PassThroughMetricProcessor implements MetricProcessor
{
    public function __construct(
        private Exporter $exporter,
    ) {
    }

    public function exporter() : Exporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        return true;
    }

    public function process(Metric $metric) : void
    {
        $this->exporter->export(Signals::metrics([$metric]));
    }
}
