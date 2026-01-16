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
 * Example implementation for PSR-7:
 * ```php
 * final class Psr7Carrier implements Carrier
 * {
 *     public function __construct(private RequestInterface $request) {}
 *
 *     public function get(string $key): ?string
 *     {
 *         $values = $this->request->getHeader($key);
 *         return $values[0] ?? null;
 *     }
 *
 *     public function set(string $key, string $value): void
 *     {
 *         $this->request = $this->request->withHeader($key, $value);
 *     }
 *
 *     public function keys(): array
 *     {
 *         return array_keys($this->request->getHeaders());
 *     }
 * }
 * ```
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
    public function get(string $key) : ?string;

    /**
     * Get all available keys.
     *
     * @return array<string> List of keys in the carrier
     */
    public function keys() : array;

    /**
     * Set a value.
     *
     * @param string $key The key to set
     * @param string $value The value to set
     */
    public function set(string $key, string $value) : void;
}
