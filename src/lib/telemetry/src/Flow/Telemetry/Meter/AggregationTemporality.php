<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * Aggregation temporality defines how a metric aggregation is reported over time.
 *
 * DELTA: The metric reports the change since the last report. Each value represents
 * the difference from the previous measurement period.
 *
 * CUMULATIVE: The metric reports the total accumulated value since the start of the
 * measurement period. Each value includes all previous measurements.
 *
 * Example:
 * - Counter with CUMULATIVE: 1, 3, 6, 10 (total requests at each point)
 * - Counter with DELTA: 1, 2, 3, 4 (requests since last report)
 *
 * @see https://opentelemetry.io/docs/specs/otel/metrics/data-model/#temporality
 */
enum AggregationTemporality: int
{
    case CUMULATIVE = 2;
    case DELTA = 1;
}
