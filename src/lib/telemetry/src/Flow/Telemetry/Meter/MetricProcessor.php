<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\Exporter\Exporter;

/**
 * Interface for processing metric measurements.
 *
 * Implementations may collect metrics for batching, export them immediately,
 * or perform other processing like filtering or aggregation.
 */
interface MetricProcessor
{
    /**
     * Get the exporter used by this processor.
     */
    public function exporter() : Exporter;

    /**
     * Export all pending metrics.
     *
     * Forces immediate export of any buffered metrics.
     *
     * @return bool True if all metrics were successfully exported
     */
    public function flush() : bool;

    /**
     * Process a metric measurement.
     *
     * This is invoked when an instrument records a value. The processor
     * may buffer the metric, export it immediately, or discard it based
     * on filtering rules.
     */
    public function process(Metric $metric) : void;
}
