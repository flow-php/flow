<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

/**
 * Interface for processing log records.
 *
 * Implementations may collect logs for batching, export them immediately,
 * filter by severity, or perform other processing like enrichment.
 *
 * Example implementation:
 * ```php
 * final class BatchingLogProcessor implements LogProcessor
 * {
 *     private array $buffer = [];
 *
 *     public function process(LogEntry $entry): void {
 *         $this->buffer[] = $entry;
 *         if (count($this->buffer) >= 100) {
 *             $this->flush();
 *         }
 *     }
 *     // ...
 * }
 * ```
 */
interface LogProcessor
{
    /**
     * Get the exporter used by this processor.
     */
    public function exporter() : LogExporter;

    /**
     * Export all pending log records.
     *
     * Forces immediate export of any buffered log records.
     *
     * @return bool True if all records were successfully exported
     */
    public function flush() : bool;

    /**
     * Process a log entry.
     *
     * This is invoked synchronously when a log is emitted. The processor
     * may buffer the entry, export it immediately, or discard it based
     * on filtering rules.
     *
     * @param LogEntry $entry The complete log entry to process
     */
    public function process(LogEntry $entry) : void;
}
