<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

final readonly class FilesystemTelemetryOptions
{
    public function __construct(
        public bool $traceStreams = true,
        public bool $collectMetrics = true,
    ) {
    }

    public function collectMetrics(bool $collect = true) : self
    {
        return new self(
            $this->traceStreams,
            $collect,
        );
    }

    public function traceStreams(bool $trace = true) : self
    {
        return new self(
            $trace,
            $this->collectMetrics,
        );
    }
}
