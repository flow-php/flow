<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

interface Propagator
{
    /**
     * Extract context from a carrier.
     *
     * @param Carrier<mixed> $carrier The carrier to extract from
     *
     * @return PropagationContext The extracted context
     */
    public function extract(Carrier $carrier): PropagationContext;

    /**
     * Get the list of header/field names this propagator uses.
     *
     * This is useful for CORS configuration and carrier filtering.
     *
     * @return array<string> List of field names
     */
    public function fields(): array;

    /**
     * Inject context into a carrier.
     *
     * @param PropagationContext $context The context to inject
     * @param Carrier<mixed> $carrier The carrier to inject into
     */
    public function inject(PropagationContext $context, Carrier $carrier): void;
}
