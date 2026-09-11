<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\HTTP\Tests\Double;

use Flow\ETL\Adapter\Http\DynamicExtractor\NextRequestFactory;
use Nyholm\Psr7\Factory\Psr17Factory;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final class NumberedRequestFactory implements NextRequestFactory
{
    private int $created = 0;

    public function __construct(
        private readonly string $baseUri,
        private readonly int $requests,
    ) {}

    public function create(?ResponseInterface $previousResponse = null): ?RequestInterface
    {
        return $this->created < $this->requests
            ? (new Psr17Factory())->createRequest('GET', $this->baseUri . ++$this->created)
            : null;
    }
}
