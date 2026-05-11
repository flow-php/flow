<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

/**
 * Combines multiple Propagators into a single propagator.
 *
 * CompositePropagator allows using multiple propagation formats simultaneously.
 * On inject, all propagators are invoked. On extract, all propagators are
 * invoked and their contexts are merged.
 *
 * Example usage:
 * ```php
 * $propagator = new CompositePropagator([
 *     new W3CTraceContext(),
 *     new W3CBaggage(),
 * ]);
 *
 * // Will inject both trace context and baggage headers
 * $carrier = new ArrayCarrier();
 * $propagator->inject($ctx, $carrier);
 * $headers = $carrier->unwrap();
 *
 * // Will extract both SpanContext and Baggage into PropagationContext
 * $carrier = new ArrayCarrier($headers);
 * $ctx = $propagator->extract($carrier);
 * $spanContext = $ctx->spanContext;
 * $baggage = $ctx->baggage;
 * ```
 */
final readonly class CompositePropagator implements Propagator
{
    /**
     * @param array<Propagator> $propagators
     */
    public function __construct(
        private array $propagators,
    ) {}

    /**
     * @param Carrier<mixed> $carrier
     */
    public function extract(Carrier $carrier): PropagationContext
    {
        $result = new PropagationContext();

        foreach ($this->propagators as $propagator) {
            $result = $result->merge($propagator->extract($carrier));
        }

        return $result;
    }

    /**
     * @return array<string>
     */
    public function fields(): array
    {
        $fields = [];

        foreach ($this->propagators as $propagator) {
            $fields = \array_merge($fields, $propagator->fields());
        }

        return \array_unique($fields);
    }

    /**
     * @param Carrier<mixed> $carrier
     */
    public function inject(PropagationContext $context, Carrier $carrier): void
    {
        foreach ($this->propagators as $propagator) {
            $propagator->inject($context, $carrier);
        }
    }
}
