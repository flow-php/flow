<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry;

use Flow\Telemetry\Propagation\Carrier;
use Symfony\Component\HttpFoundation\Response;

/**
 * Read-write carrier backed by Symfony HttpFoundation Response.
 *
 * Reads and writes context to HTTP response headers.
 *
 * Example usage:
 * ```php
 * $propagator = new CompositePropagator([
 *     new W3CTraceContext(),
 *     new W3CBaggage(),
 * ]);
 *
 * $carrier = new ResponseCarrier($response);
 * $propagator->inject($spanContext, $carrier);
 * $response = $carrier->unwrap();
 *
 * // Or with fluent chaining
 * $response = (new ResponseCarrier($response))
 *     ->set('X-Custom', 'value')
 *     ->unwrap();
 * ```
 *
 * @implements Carrier<Response>
 */
final readonly class ResponseCarrier implements Carrier
{
    public function __construct(
        private Response $response,
    ) {}

    public function get(string $key): ?string
    {
        return $this->response->headers->get($key);
    }

    public function set(string $key, string $value): static
    {
        $this->response->headers->set($key, $value);

        return $this;
    }

    public function unwrap(): Response
    {
        return $this->response;
    }
}
