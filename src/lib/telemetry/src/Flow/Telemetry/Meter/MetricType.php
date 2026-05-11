<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * Defines the type of metric instrument.
 *
 * Each type has different semantics for how values are recorded
 * and aggregated over time.
 */
enum MetricType: string
{
    /**
     * Monotonically increasing counter.
     *
     * Values can only increase (or stay the same).
     */
    case COUNTER = 'counter';

    /**
     * Point-in-time gauge measurement.
     *
     * Captures current value at a moment in time.
     */
    case GAUGE = 'gauge';

    /**
     * Distribution of values (histogram).
     *
     * Tracks statistical distribution of measurements.
     */
    case HISTOGRAM = 'histogram';

    /**
     * Counter that can increase or decrease.
     *
     * Values can go up or down.
     */
    case UP_DOWN_COUNTER = 'up_down_counter';
}
