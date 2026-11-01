<?php

declare(strict_types=1);

namespace Flow\Bridge\Psr7\Telemetry;

use Flow\Bridge\Psr7\Telemetry\Exception\RuntimeException;
use Flow\Telemetry\Propagation\Carrier;
use Psr\Http\Message\ServerRequestInterface;

/**
 * Read-only: set() throws.
 *
 * @implements Carrier<ServerRequestInterface>
 */
final readonly class RequestCarrier implements Carrier
{
    public function __construct(
        private ServerRequestInterface $request,
    ) {}

    public function get(string $key): ?string
    {
        return $this->request->getHeader($key)[0] ?? null;
    }

    public function set(string $key, string $value): static
    {
        throw new RuntimeException('RequestCarrier is read-only');
    }

    public function unwrap(): ServerRequestInterface
    {
        return $this->request;
    }
}
