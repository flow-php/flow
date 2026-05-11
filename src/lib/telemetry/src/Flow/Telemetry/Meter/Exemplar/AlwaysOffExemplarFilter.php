<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Never records exemplars.
 *
 * This filter disables exemplar collection entirely.
 * Use this when you don't need trace-to-metric correlation
 * or want to minimize overhead.
 */
final readonly class AlwaysOffExemplarFilter implements ExemplarFilter
{
    public function __toString(): string
    {
        return 'AlwaysOffExemplarFilter';
    }

    public function shouldSample(?SpanContext $context, int|float $value, array $attributes): bool
    {
        return false;
    }
}
