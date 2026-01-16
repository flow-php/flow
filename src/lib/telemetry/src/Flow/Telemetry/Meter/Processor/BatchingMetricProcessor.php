<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\Meter\{Metric, MetricExporter, MetricProcessor};

/**
 * Batches metrics for efficient export.
 *
 * Collects metrics in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 *
 * Example usage:
 * ```php
 * $processor = new BatchingMetricProcessor(
 *     exporter: $metricExporter,
 *     batchSize: 100,
 * );
 * ```
 */
final class BatchingMetricProcessor implements MetricProcessor
{
    /**
     * @var array<Metric>
     */
    private array $buffer = [];

    public function __construct(
        private readonly MetricExporter $exporter,
        private readonly int $batchSize = 512,
    ) {
    }

    public function exporter() : MetricExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        if (\count($this->buffer) === 0) {
            return true;
        }

        $metrics = $this->buffer;
        $this->buffer = [];

        return $this->exporter->export($metrics);
    }

    public function process(Metric $metric) : void
    {
        $this->buffer[] = $metric;

        if (\count($this->buffer) >= $this->batchSize) {
            $this->flush();
        }
    }
}
