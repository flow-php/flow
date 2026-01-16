<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

/**
 * Carrier backed by an associative array.
 *
 * Provides case-insensitive key lookup for HTTP header compatibility.
 * Keys are stored in their original case but lookups ignore case.
 *
 * Example usage:
 * ```php
 * // Extract from incoming request headers
 * $carrier = new ArrayCarrier($request->getHeaders());
 * $spanContext = $propagator->extract($carrier);
 *
 * // Inject into outgoing request
 * $carrier = new ArrayCarrier();
 * $propagator->inject($spanContext, $carrier);
 * $headers = $carrier->toArray();
 * ```
 */
final class ArrayCarrier implements Carrier
{
    /**
     * @param array<string, string> $data The carrier data
     */
    public function __construct(
        private array $data = [],
    ) {
    }

    public function get(string $key) : ?string
    {
        $lowerKey = \strtolower($key);

        foreach ($this->data as $k => $v) {
            if (\strtolower($k) === $lowerKey) {
                return $v;
            }
        }

        return null;
    }

    public function set(string $key, string $value) : void
    {
        $this->data[$key] = $value;
    }

    /**
     * Get the carrier data as an array.
     *
     * @return array<string, string>
     */
    public function toArray() : array
    {
        return $this->data;
    }
}
