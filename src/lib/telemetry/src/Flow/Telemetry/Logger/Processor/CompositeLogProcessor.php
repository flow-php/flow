<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\{LogEntry, LogProcessor};

/**
 * Forwards log records to multiple processors.
 *
 * This is useful when you need to:
 * - Send logs to multiple backends (e.g., both console and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 */
final readonly class CompositeLogProcessor implements LogProcessor
{
    /**
     * @param array<LogProcessor> $processors
     */
    public function __construct(
        private array $processors,
        private ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {
    }

    public function exporter() : Exporter
    {
        if (\count($this->processors) === 0) {
            throw new \RuntimeException('CompositeLogProcessor has no processors');
        }

        return $this->processors[0]->exporter();
    }

    public function flush() : bool
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

    public function process(LogEntry $entry) : void
    {
        foreach ($this->processors as $processor) {
            try {
                $processor->process($entry);
            } catch (\Throwable $e) {
                $this->errorHandler->handle($e);
            }
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<LogProcessor>
     */
    public function processors() : array
    {
        return $this->processors;
    }
}
