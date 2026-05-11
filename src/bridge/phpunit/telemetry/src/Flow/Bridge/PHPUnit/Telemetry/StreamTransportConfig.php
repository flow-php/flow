<?php

declare(strict_types=1);

namespace Flow\Bridge\PHPUnit\Telemetry;

final readonly class StreamTransportConfig
{
    public function __construct(
        public string $destination,
        public int $filePermissions,
        public bool $createDirectories,
    ) {}
}
