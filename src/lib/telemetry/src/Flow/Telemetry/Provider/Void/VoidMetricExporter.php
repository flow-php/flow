<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Meter\{Metric, MetricExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * No-op metric exporter that discards all data.
 */
final readonly class VoidMetricExporter implements MetricExporter
{
    /**
     * @param array<Metric> $metrics
     */
    public function export(array $metrics) : bool
    {
        return true;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
