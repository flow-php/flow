<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

/**
 * Carrier for propagating context across process boundaries.
 *
 * A carrier is the transport mechanism for context propagation headers.
 * Implementations handle specific transport types (HTTP headers, gRPC
 * metadata, message queue headers, etc.).
 *
 * Example usage:
 * ```php
 * // Inject context into a carrier
 * $carrier = new ResponseCarrier($response);
 * $propagator->inject($context, $carrier);
 * $modifiedResponse = $carrier->unwrap();
 *
 * // With fluent chaining
 * $response = (new ResponseCarrier($response))
 *     ->set('X-Custom', 'value')
 *     ->unwrap();
 * ```
 *
 * @template-covariant TWrapped
 */
interface Carrier
{
    /**
     * Get a value by key.
     *
     * Key lookup should be case-insensitive for HTTP header compatibility.
     *
     * @param string $key The key to look up
     *
     * @return null|string The value, or null if not found
     */
    public function get(string $key): ?string;

    /**
     * Set a value.
     *
     * @param string $key The key to set
     * @param string $value The value to set
     *
     * @return static For fluent chaining
     */
    public function set(string $key, string $value): static;

    /**
     * Unwrap and return the underlying data structure.
     *
     * @return TWrapped The wrapped data structure
     */
    public function unwrap(): mixed;
}
