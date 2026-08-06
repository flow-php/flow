<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Psr18;

use Nyholm\Psr7\Response;
use Psr\Http\Client\ClientInterface;
use Psr\Http\Message\RequestInterface;
use Psr\Http\Message\ResponseInterface;
use Symfony\Contracts\Service\ResetInterface;

final readonly class ResettablePsr18Client implements ClientInterface, ResetInterface
{
    public function sendRequest(RequestInterface $request): ResponseInterface
    {
        return new Response(200);
    }

    public function reset(): void {}
}
