<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

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
