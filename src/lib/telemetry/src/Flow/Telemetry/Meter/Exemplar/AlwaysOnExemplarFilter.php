<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Records an exemplar when span context is available.
 *
 * This filter captures exemplars for all measurements that have
 * an associated span context, regardless of whether the span is sampled.
 * Use this when you want maximum observability and can afford
 * the storage overhead.
 */
final readonly class AlwaysOnExemplarFilter implements ExemplarFilter
{
    public function __toString(): string
    {
        return 'AlwaysOnExemplarFilter';
    }

    public function shouldSample(?SpanContext $context, int|float $value, array $attributes): bool
    {
        if ($context === null) {
            return false;
        }

        return true;
    }
}
