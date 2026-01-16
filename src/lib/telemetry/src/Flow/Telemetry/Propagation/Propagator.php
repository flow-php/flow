<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

/**
 * Interface for propagating context across process boundaries.
 *
 * Propagator defines how context information (like trace identifiers and
 * baggage) is injected into and extracted from carriers such as HTTP headers,
 * gRPC metadata, or message queue headers.
 *
 * Example usage:
 * ```php
 * $propagator = new W3CTraceContext();
 *
 * // Extract context from incoming request headers
 * $carrier = new ArrayCarrier($incomingHeaders);
 * $ctx = $propagator->extract($carrier);
 *
 * // Inject context into outgoing request headers
 * $carrier = new ArrayCarrier();
 * $propagator->inject($ctx, $carrier);
 * $headers = $carrier->toArray();
 * ```
 */
interface Propagator
{
    /**
     * Extract context from a carrier.
     *
     * @param Carrier $carrier The carrier to extract from
     *
     * @return PropagationContext The extracted context
     */
    public function extract(Carrier $carrier) : PropagationContext;

    /**
     * Get the list of header/field names this propagator uses.
     *
     * This is useful for CORS configuration and carrier filtering.
     *
     * @return array<string> List of field names
     */
    public function fields() : array;

    /**
     * Inject context into a carrier.
     *
     * @param PropagationContext $context The context to inject
     * @param Carrier $carrier The carrier to inject into
     */
    public function inject(PropagationContext $context, Carrier $carrier) : void;
}
