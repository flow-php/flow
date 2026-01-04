<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Instrument;

use Flow\Telemetry\Meter\Metric;

/**
 * Base interface for metric instruments.
 *
 * Instruments are responsible for recording measurements and aggregating
 * them internally. Each instrument type has different aggregation semantics:
 *
 * - Counter: Sum aggregation (monotonically increasing)
 * - UpDownCounter: Sum aggregation (bidirectional)
 * - Gauge: Last value aggregation
 * - Histogram: Distribution aggregation (count, sum, min, max)
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/api/
 */
interface Instrument
{
    /**
     * Collect aggregated metrics for export.
     *
     * Returns all aggregated metrics since the last collection,
     * then resets the internal aggregation state.
     *
     * @return array<Metric> Aggregated metrics ready for export
     */
    public function collect() : array;

    /**
     * Get the instrument description.
     */
    public function description() : ?string;

    /**
     * Get the instrument name.
     */
    public function name() : string;

    /**
     * Get the unit of measurement.
     */
    public function unit() : ?string;
}
