<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor};

/**
 * Batches log records for efficient export.
 *
 * Collects log records in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 *
 * Example usage:
 * ```php
 * $processor = new BatchingLogProcessor(
 *     exporter: $logExporter,
 *     batchSize: 100,
 * );
 * ```
 */
final class BatchingLogProcessor implements LogProcessor
{
    /**
     * @var array<LogEntry>
     */
    private array $buffer = [];

    public function __construct(
        private readonly LogExporter $exporter,
        private readonly int $batchSize = 512,
    ) {
    }

    public function exporter() : LogExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        if (\count($this->buffer) === 0) {
            return true;
        }

        $entries = $this->buffer;
        $this->buffer = [];

        return $this->exporter->export($entries);
    }

    public function process(LogEntry $entry) : void
    {
        $this->buffer[] = $entry;

        if (\count($this->buffer) >= $this->batchSize) {
            $this->flush();
        }
    }
}
