<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry;

use Flow\Telemetry\Propagation\Carrier;
use Psr\Http\Message\ResponseInterface;

/**
 * Read-write carrier backed by PSR-7 ResponseInterface.
 *
 * Reads and writes context to HTTP response headers.
 * Due to PSR-7's immutability, use unwrap() to retrieve
 * the modified response after calling set().
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
 * $response = $carrier->unwrap(); // Get modified response
 *
 * // Or with fluent chaining
 * $response = (new ResponseCarrier($response))
 *     ->set('X-Custom', 'value')
 *     ->unwrap();
 * ```
 *
 * @implements Carrier<ResponseInterface>
 */
final class ResponseCarrier implements Carrier
{
    public function __construct(
        private ResponseInterface $response,
    ) {
    }

    public function get(string $key) : ?string
    {
        return $this->response->getHeader($key)[0] ?? null;
    }

    public function set(string $key, string $value) : static
    {
        $this->response = $this->response->withHeader($key, $value);

        return $this;
    }

    public function unwrap() : ResponseInterface
    {
        return $this->response;
    }
}
