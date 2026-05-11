<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use Flow\Telemetry\Context\Baggage;
use Flow\Telemetry\Tracer\SpanContext;

/**
 * Value object containing propagated context information.
 *
 * PropagationContext holds both trace context (SpanContext) and application
 * data (Baggage) that can be propagated across process boundaries.
 *
 * Example usage:
 * ```php
 * $propagator = new CompositePropagator([
 *     new W3CTraceContext(),
 *     new W3CBaggage(),
 * ]);
 *
 * $ctx = $propagator->extract($carrier);
 * $spanContext = $ctx->spanContext;
 * $baggage = $ctx->baggage;
 * ```
 */
final readonly class PropagationContext
{
    public function __construct(
        public ?SpanContext $spanContext = null,
        public ?Baggage $baggage = null,
    ) {}

    /**
     * Merge this context with another, preferring values from the other context.
     */
    public function merge(self $other): self
    {
        return new self($other->spanContext ?? $this->spanContext, $other->baggage ?? $this->baggage);
    }

    /**
     * Create a new context with a different baggage.
     */
    public function withBaggage(?Baggage $baggage): self
    {
        return new self($this->spanContext, $baggage);
    }

    /**
     * Create a new context with a different span context.
     */
    public function withSpanContext(?SpanContext $spanContext): self
    {
        return new self($spanContext, $this->baggage);
    }
}
