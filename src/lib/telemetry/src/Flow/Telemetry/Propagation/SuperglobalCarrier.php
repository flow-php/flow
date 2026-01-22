<?php

declare(strict_types=1);

namespace Flow\Telemetry\Propagation;

use Flow\Telemetry\Exception\RuntimeException;

/**
 * Read-only carrier backed by PHP superglobals.
 *
 * Reads context from $_SERVER (HTTP headers), $_GET, $_POST, and $_COOKIE.
 * This carrier is read-only and will throw if set() is called.
 *
 * HTTP headers in $_SERVER are normalized from HTTP_* format:
 * - HTTP_TRACEPARENT becomes 'traceparent'
 * - HTTP_X_CUSTOM_HEADER becomes 'x-custom-header'
 *
 * Example usage:
 * ```php
 * $propagator = new CompositePropagator([
 *     new W3CTraceContext(),
 *     new W3CBaggage(),
 * ]);
 *
 * $carrier = new SuperglobalCarrier();
 * $ctx = $propagator->extract($carrier);
 * ```
 *
 * @implements Carrier<array<string, string>>
 */
final readonly class SuperglobalCarrier implements Carrier
{
    /**
     * @var array<string, string>
     */
    private array $data;

    public function __construct()
    {
        $data = [];

        foreach ($_COOKIE as $k => $v) {
            if (\is_string($k) && \is_string($v)) {
                $data[\strtolower($k)] = $v;
            }
        }

        foreach ($_POST as $k => $v) {
            if (\is_string($k) && \is_string($v)) {
                $data[\strtolower($k)] = $v;
            }
        }

        foreach ($_GET as $k => $v) {
            if (\is_string($k) && \is_string($v)) {
                $data[\strtolower($k)] = $v;
            }
        }

        foreach ($_SERVER as $key => $value) {
            if (\is_string($key) && \str_starts_with($key, 'HTTP_') && \is_string($value)) {
                $headerName = \strtolower(\str_replace('_', '-', \substr($key, 5)));
                $data[$headerName] = $value;
            }
        }

        $this->data = $data;
    }

    public function get(string $key) : ?string
    {
        $lowerKey = \strtolower($key);

        return $this->data[$lowerKey] ?? null;
    }

    public function set(string $key, string $value) : static
    {
        throw new RuntimeException('SuperglobalCarrier is read-only');
    }

    /**
     * @return array<string, string>
     */
    public function unwrap() : array
    {
        return $this->data;
    }
}
