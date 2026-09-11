<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

final readonly class TelemetryOptions
{
    public function __construct(
        public bool $traceLoading = false,
        public bool $traceTransformations = false,
        public bool $traceCache = false,
        public bool $collectMetrics = false,
    ) {}

    public function collectMetrics(bool $collect = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            traceCache: $this->traceCache,
            collectMetrics: $collect,
        );
    }

    public function traceCache(bool $trace = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            traceCache: $trace,
            collectMetrics: $this->collectMetrics,
        );
    }

    public function traceLoading(bool $trace = true): self
    {
        return new self(
            traceLoading: $trace,
            traceTransformations: $this->traceTransformations,
            traceCache: $this->traceCache,
            collectMetrics: $this->collectMetrics,
        );
    }

    public function traceTransformations(bool $trace = true): self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $trace,
            traceCache: $this->traceCache,
            collectMetrics: $this->collectMetrics,
        );
    }
}
