<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\HttpFoundationTelemetry;

use Flow\Bridge\Symfony\HttpFoundationTelemetry\Exception\RuntimeException;
use Flow\Telemetry\Propagation\Carrier;
use Symfony\Component\HttpFoundation\Request;

use function is_string;

/**
 * Read-only carrier backed by the Symfony HttpFoundation Request query string.
 *
 * @implements Carrier<Request>
 */
final readonly class QueryCarrier implements Carrier
{
    public function __construct(
        private Request $request,
    ) {}

    public function get(string $key): ?string
    {
        // query->get() throws on array values; read the raw bag so array params degrade to null instead.
        // @mago-expect analysis:mixed-assignment
        $value = $this->request->query->all()[$key] ?? null;

        return is_string($value) ? $value : null;
    }

    public function set(string $key, string $value): static
    {
        throw new RuntimeException('QueryCarrier is read-only');
    }

    public function unwrap(): Request
    {
        return $this->request;
    }
}
