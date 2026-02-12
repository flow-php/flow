<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Telemetry\Telemetry;
use Psr\Clock\ClockInterface;

final readonly class FilesystemTelemetryConfig
{
    public function __construct(
        public Telemetry $telemetry,
        public ClockInterface $clock,
        public FilesystemTelemetryOptions $options = new FilesystemTelemetryOptions(),
    ) {
    }
}
