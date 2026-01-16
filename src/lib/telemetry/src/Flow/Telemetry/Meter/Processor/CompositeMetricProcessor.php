<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricProcessor};

/**
 * Forwards metrics to multiple processors.
 *
 * This is useful when you need to:
 * - Send metrics to multiple backends (e.g., both Prometheus and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 *
 * Example usage:
 * ```php
 * $processor = new CompositeMetricProcessor([
 *     new BatchingMetricProcessor($otlpExporter),
 *     new MemoryMetricProcessor(),
 * ]);
 * ```
 */
final readonly class CompositeMetricProcessor implements MetricProcessor
{
    /**
     * @param array<MetricProcessor> $processors
     */
    public function __construct(
        private array $processors,
    ) {
    }

    public function exporter() : MetricExporter
    {
        if (\count($this->processors) === 0) {
            throw new \RuntimeException('CompositeMetricProcessor has no processors');
        }

        return $this->processors[0]->exporter();
    }

    public function flush() : bool
    {
        $success = true;

        foreach ($this->processors as $processor) {
            if (!$processor->flush()) {
                $success = false;
            }
        }

        return $success;
    }

    public function process(Metric $metric) : void
    {
        foreach ($this->processors as $processor) {
            $processor->process($metric);
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<MetricProcessor>
     */
    public function processors() : array
    {
        return $this->processors;
    }
}
