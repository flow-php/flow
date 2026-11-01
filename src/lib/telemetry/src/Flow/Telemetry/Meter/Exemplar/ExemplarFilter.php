<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;
use Stringable;

interface ExemplarFilter extends Stringable
{
    /**
     * Determine whether to record an exemplar for this measurement.
     *
     * @param null|SpanContext $context The current span context, or null if none
     * @param float|int $value The measurement value
     * @param array<string, mixed> $attributes The measurement attributes
     */
    public function shouldSample(?SpanContext $context, int|float $value, array $attributes): bool;
}
