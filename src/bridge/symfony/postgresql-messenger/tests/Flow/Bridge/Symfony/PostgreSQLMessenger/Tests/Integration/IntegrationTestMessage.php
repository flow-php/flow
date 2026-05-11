<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\PostgreSQLMessenger\Tests\Integration;

final readonly class IntegrationTestMessage
{
    public function __construct(
        public string $payload,
    ) {}
}
