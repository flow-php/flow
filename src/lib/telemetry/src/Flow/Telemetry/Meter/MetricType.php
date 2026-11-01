<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

enum MetricType: string
{
    /**
     * Monotonically increasing counter.
     */
    case COUNTER = 'counter';

    /**
     * Point-in-time gauge measurement.
     */
    case GAUGE = 'gauge';

    /**
     * Distribution of values (histogram).
     */
    case HISTOGRAM = 'histogram';

    /**
     * Counter that can increase or decrease.
     */
    case UP_DOWN_COUNTER = 'up_down_counter';
}
