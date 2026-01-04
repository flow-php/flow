<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Exemplar;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Records an exemplar only when the span is sampled.
 *
 * This is the default filter per OpenTelemetry specification.
 * It captures exemplars only for measurements where the associated
 * span has the SAMPLED trace flag set. This provides trace-metric
 * correlation while keeping storage costs aligned with trace sampling.
 */
final readonly class TraceBasedExemplarFilter implements ExemplarFilter
{
    public function __toString() : string
    {
        return 'TraceBasedExemplarFilter';
    }

    public function shouldSample(?SpanContext $context, int|float $value, array $attributes) : bool
    {
        if ($context === null) {
            return false;
        }

        return $context->traceFlags->isSampled();
    }
}
