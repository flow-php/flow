<?php

declare(strict_types=1);

namespace Flow\Azure\SDK;

use DateTimeImmutable;
use Psr\Http\Message\RequestInterface;

interface AuthorizationFactory
{
    public function for(RequestInterface $request): string;

    public function readOnlySourceUrl(
        string $url,
        string $container,
        string $blob,
        DateTimeImmutable $expiresAt,
    ): string;
}
