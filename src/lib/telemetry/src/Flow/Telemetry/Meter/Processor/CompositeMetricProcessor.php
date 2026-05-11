<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;

/**
 * Forwards metrics to multiple processors.
 *
 * This is useful when you need to:
 * - Send metrics to multiple backends (e.g., both Prometheus and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 */
final readonly class CompositeMetricProcessor implements MetricProcessor
{
    /**
     * @param array<MetricProcessor> $processors
     */
    public function __construct(
        private array $processors,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        $success = true;

        foreach ($this->processors as $processor) {
            try {
                if (!$processor->flush()) {
                    $success = false;
                }
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
                $success = false;
            }
        }

        return $success;
    }

    public function process(Metric $metric): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->process($metric);
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<MetricProcessor>
     */
    public function processors(): array
    {
        return $this->processors;
    }

    public function shutdown(): void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->shutdown();
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }
}
