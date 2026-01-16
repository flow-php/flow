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
 */
final readonly class SuperglobalCarrier implements Carrier
{
    public function get(string $key) : ?string
    {
        $httpKey = 'HTTP_' . \strtoupper(\str_replace('-', '_', $key));

        if (isset($_SERVER[$httpKey]) && \is_string($_SERVER[$httpKey])) {
            return $_SERVER[$httpKey];
        }

        $lowerKey = \strtolower($key);

        foreach ($_GET as $k => $v) {
            if (\strtolower((string) $k) === $lowerKey && \is_string($v)) {
                return $v;
            }
        }

        foreach ($_POST as $k => $v) {
            if (\strtolower((string) $k) === $lowerKey && \is_string($v)) {
                return $v;
            }
        }

        foreach ($_COOKIE as $k => $v) {
            if (\strtolower((string) $k) === $lowerKey && \is_string($v)) {
                return $v;
            }
        }

        return null;
    }

    public function set(string $key, string $value) : void
    {
        throw new RuntimeException('SuperglobalCarrier is read-only');
    }
}
