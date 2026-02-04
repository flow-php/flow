<?php

declare(strict_types=1);

namespace Flow\ETL\Config\Telemetry;

final readonly class TelemetryOptions
{
    public function __construct(
        public bool $traceLoading = false,
        public bool $traceTransformations = false,
        public bool $collectMetrics = false,
    ) {
    }

    public function collectMetrics(bool $collect) : self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $this->traceTransformations,
            collectMetrics: $collect
        );
    }

    public function traceLoading(bool $trace) : self
    {
        return new self(
            traceLoading: $trace,
            traceTransformations: $this->traceTransformations,
            collectMetrics: $this->collectMetrics
        );
    }

    public function traceTransformations(bool $trace) : self
    {
        return new self(
            traceLoading: $this->traceLoading,
            traceTransformations: $trace,
            collectMetrics: $this->collectMetrics
        );
    }
}
