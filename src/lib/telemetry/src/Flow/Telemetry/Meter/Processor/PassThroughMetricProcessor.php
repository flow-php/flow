<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter\Processor;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Signal\Signals;

/**
 * Exports each metric immediately when processed.
 *
 * Unlike BatchingMetricProcessor, this processor exports metrics synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of metrics is more important than performance.
 */
final readonly class PassThroughMetricProcessor implements MetricProcessor
{
    public function __construct(
        private Exporter $exporter,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function flush(): bool
    {
        return true;
    }

    public function process(Metric $metric): void
    {
        try {
            $this->exporter->export(Signals::metrics([$metric]));
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }

    public function shutdown(): void
    {
        try {
            $this->exporter->shutdown();
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
