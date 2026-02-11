<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

use Flow\Telemetry\Telemetry;

final readonly class FilesystemTelemetryConfig
{
    public function __construct(
        public Telemetry $telemetry,
        public FilesystemTelemetryOptions $options = new FilesystemTelemetryOptions(),
    ) {
    }
}
