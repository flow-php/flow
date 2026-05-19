<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18;

use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use RuntimeException;

final readonly class FailingPsr18Client implements ClientInterface
{
    public function __construct(
        private string $message = 'Connection failed',
    ) {}

    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        throw new RuntimeException($this->message);
    }
}
