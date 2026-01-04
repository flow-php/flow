<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor};

/**
 * Forwards log records to multiple processors.
 *
 * This is useful when you need to:
 * - Send logs to multiple backends (e.g., both console and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 *
 * Example usage:
 * ```php
 * $processor = new CompositeLogProcessor([
 *     new BatchingLogProcessor($otlpExporter),
 *     new MemoryLogProcessor(),
 * ]);
 * ```
 */
final readonly class CompositeLogProcessor implements LogProcessor
{
    /**
     * @param array<LogProcessor> $processors
     */
    public function __construct(
        private array $processors,
    ) {
    }

    public function exporter() : LogExporter
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
            if (!$processor->flush()) {
                $success = false;
            }
        }

        return $success;
    }

    public function process(LogEntry $entry) : void
    {
        foreach ($this->processors as $processor) {
            $processor->process($entry);
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
