<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

/**
 * @see https://opentelemetry.io/docs/specs/otel/metrics/data-model/#temporality
 */
enum AggregationTemporality: int
{
    case CUMULATIVE = 2;
    case DELTA = 1;
}
