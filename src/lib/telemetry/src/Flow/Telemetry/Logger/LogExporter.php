<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger;

use Flow\Telemetry\Transport\Transport;

/**
 * Interface for exporting log records to external systems.
 *
 * Exporters are responsible for transmitting log data to backends
 * like OTLP collectors, logging services, or custom storage.
 *
 * Example implementation:
 * ```php
 * final class OTLPLogExporter implements LogExporter
 * {
 *     public function export(array $entries): bool
 *     {
 *         // Convert to OTLP format and send to collector
 *         return $this->client->sendLogs($entries);
 *     }
 *     // ...
 * }
 * ```
 */
interface LogExporter
{
    /**
     * Export a batch of log entries.
     *
     * Each log entry carries its own Resource and InstrumentationScope.
     *
     * @param array<LogEntry> $entries The log entries to export
     *
     * @return bool True on success, false on failure
     */
    public function export(array $entries) : bool;

    /**
     * Get the transports used by this exporter.
     *
     * @return array<Transport> Always returns at least one transport
     */
    public function transports() : array;
}
