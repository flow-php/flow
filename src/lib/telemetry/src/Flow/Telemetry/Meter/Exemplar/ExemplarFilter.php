<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Determines whether an exemplar should be recorded for a metric measurement.
 *
 * ExemplarFilter is used to decide when to capture trace context alongside
 * metric data points. This enables drill-down from metrics to traces when
 * exported to backends that support exemplars.
 *
 * Example usage:
 * ```php
 * $filter = new TraceBasedExemplarFilter();
 * if ($filter->shouldSample($spanContext, 100, ['status' => 'ok'])) {
 *     // Record exemplar with trace context
 * }
 * ```
 */
interface ExemplarFilter extends \Stringable
{
    /**
     * Determine whether to record an exemplar for this measurement.
     *
     * @param null|SpanContext $context The current span context, or null if none
     * @param float|int $value The measurement value
     * @param array<string, array<bool|float|int|string>|bool|float|int|string> $attributes The measurement attributes
     */
    public function shouldSample(?SpanContext $context, int|float $value, array $attributes) : bool;
}
