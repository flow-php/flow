<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

use Flow\Filesystem\Telemetry\FilesystemTelemetryOptions;

final readonly class TelemetryOptions
{
    public function __construct(
        public bool $traceLoading = false,
        public bool $traceTransformations = false,
        public bool $traceCache = false,
        public bool $collectMetrics = false,
        public FilesystemTelemetryOptions $filesystem = new FilesystemTelemetryOptions(),
    ) {}

    public function collectMetrics(bool $collect = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            traceCache: $this->traceCache,
            collectMetrics: $collect,
            filesystem: $this->filesystem,
        );
    }

    public function filesystem(FilesystemTelemetryOptions $options): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            traceCache: $this->traceCache,
            collectMetrics: $this->collectMetrics,
            filesystem: $options,
        );
    }

    public function traceCache(bool $trace = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            traceCache: $trace,
            collectMetrics: $this->collectMetrics,
            filesystem: $this->filesystem,
        );
    }

    public function traceLoading(bool $trace = true): self
    {
        return new self(
            traceLoading: $trace,
            traceTransformations: $this->traceTransformations,
            traceCache: $this->traceCache,
            collectMetrics: $this->collectMetrics,
            filesystem: $this->filesystem,
        );
    }

    public function traceTransformations(bool $trace = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $trace,
            traceCache: $this->traceCache,
            collectMetrics: $this->collectMetrics,
            filesystem: $this->filesystem,
        );
    }
}
