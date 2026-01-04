<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricProcessor};

/**
 * No-op metric processor that discards all data.
 */
final readonly class VoidMetricProcessor implements MetricProcessor
{
    public function exporter() : MetricExporter
    {
        return new VoidMetricExporter();
    }

    public function flush() : bool
    {
        return true;
    }

    public function process(Metric $metric) : void
    {
    }
}
