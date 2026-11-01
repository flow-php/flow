<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Needs a span context; the sampled flag is ignored.
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
