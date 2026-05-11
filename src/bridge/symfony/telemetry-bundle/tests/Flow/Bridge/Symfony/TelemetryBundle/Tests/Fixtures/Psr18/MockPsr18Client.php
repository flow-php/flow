<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18;

use Nyholm\Psr7\Response;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;

final readonly class MockPsr18Client implements ClientInterface
{
    public function __construct(
        private int $statusCode = 200,
    ) {}

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        return new Response($this->statusCode);
    }
}
