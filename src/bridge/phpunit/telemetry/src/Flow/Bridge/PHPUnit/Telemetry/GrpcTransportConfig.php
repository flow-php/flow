<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

final readonly class GrpcTransportConfig
{
    /**
     * @param array<string, string> $headers
     */
    public function __construct(
        public string $endpoint,
        public array $headers,
        public bool $insecure,
        public int $timeoutMs,
        public int $shutdownTimeoutMs,
    ) {
    }
}
