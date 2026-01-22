<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\Exception\RuntimeException;
use Flow\Telemetry\Propagation\Carrier;
use Symfony\Component\HttpFoundation\Request;

/**
 * Read-only carrier backed by Symfony HttpFoundation Request.
 *
 * Reads context from HTTP request headers. This carrier is read-only
 * and will throw if set() is called.
 *
 * Example usage:
 * ```php
 * $propagator = new CompositePropagator([
 *     new W3CTraceContext(),
 *     new W3CBaggage(),
 * ]);
 *
 * $carrier = new RequestCarrier($request);
 * $ctx = $propagator->extract($carrier);
 * $request = $carrier->unwrap();
 * ```
 *
 * @implements Carrier<Request>
 */
final readonly class RequestCarrier implements Carrier
{
    public function __construct(
        private Request $request,
    ) {
    }

    public function get(string $key) : ?string
    {
        return $this->request->headers->get($key);
    }

    public function set(string $key, string $value) : static
    {
        throw new RuntimeException('RequestCarrier is read-only');
    }

    public function unwrap() : Request
    {
        return $this->request;
    }
}
