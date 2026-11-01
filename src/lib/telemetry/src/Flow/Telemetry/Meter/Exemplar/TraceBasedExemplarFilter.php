<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

final readonly class TraceBasedExemplarFilter implements ExemplarFilter
{
    public function __toString(): string
    {
        return 'TraceBasedExemplarFilter';
    }

    public function shouldSample(?SpanContext $context, int|float $value, array $attributes): bool
    {
        if ($context === null) {
            return false;
        }

        return $context->traceFlags->isSampled();
    }
}
