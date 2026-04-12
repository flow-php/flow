<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client;

final readonly class Notification
{
    public function __construct(
        public string $channel,
        public string $payload,
        public int $pid,
    ) {
    }
}
