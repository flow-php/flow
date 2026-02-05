<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Message;

final readonly class TestMessage
{
    public function __construct(
        public string $content,
    ) {
    }
}
