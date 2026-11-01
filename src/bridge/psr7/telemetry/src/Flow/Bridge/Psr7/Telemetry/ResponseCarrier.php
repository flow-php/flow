<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry;

use Flow\Telemetry\Propagation\Carrier;
use Psr\Http\Message\ResponseInterface;

/**
 * PSR-7 is immutable: call unwrap() after set() to get the modified response.
 *
 * @implements Carrier<ResponseInterface>
 */
final class ResponseCarrier implements Carrier
{
    public function __construct(
        private ResponseInterface $response,
    ) {}

    public function get(string $key): ?string
    {
        return $this->response->getHeader($key)[0] ?? null;
    }

    public function set(string $key, string $value): static
    {
        $this->response = $this->response->withHeader($key, $value);

        return $this;
    }

    public function unwrap(): ResponseInterface
    {
        return $this->response;
    }
}
