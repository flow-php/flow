<?php

declare(strict_types=1);

namespace Flow\Filesystem\Telemetry;

final readonly class FilesystemTelemetryOptions
{
    public function __construct(
        public bool $traceFilesystemOperations = true,
        public bool $traceStreamOperations = true,
    ) {
    }

    public function withFilesystemOperations(bool $trace = true) : self
    {
        return new self($trace, $this->traceStreamOperations);
    }

    public function withStreamOperations(bool $trace = true) : self
    {
        return new self($this->traceFilesystemOperations, $trace);
    }
}
