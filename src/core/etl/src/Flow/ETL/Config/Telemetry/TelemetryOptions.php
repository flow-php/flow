<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

final readonly class TelemetryOptions
{
    public function __construct(
        public bool $traceLoading = false,
        public bool $traceTransformations = false,
        public bool $collectMetrics = false,
        public bool $traceFilesystem = false,
    ) {
    }

    public function collectMetrics(bool $collect = true) : self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            collectMetrics: $collect,
            traceFilesystem: $this->traceFilesystem
        );
    }

    public function traceFilesystem(bool $trace = true) : self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            collectMetrics: $this->collectMetrics,
            traceFilesystem: $trace
        );
    }

    public function traceLoading(bool $trace = true) : self
    {
        return new self(
            traceLoading: $trace,
            traceTransformations: $this->traceTransformations,
            collectMetrics: $this->collectMetrics,
            traceFilesystem: $this->traceFilesystem
        );
    }

    public function traceTransformations(bool $trace = true) : self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $trace,
            collectMetrics: $this->collectMetrics,
            traceFilesystem: $this->traceFilesystem
        );
    }
}
