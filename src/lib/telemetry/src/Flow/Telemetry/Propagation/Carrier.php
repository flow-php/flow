<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

/**
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
