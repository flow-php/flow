<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Logger\{LogEntry, LogExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * Exporter that stores log entries in memory for direct access.
 *
 * Useful for testing and inspection where you need direct access
 * to exported data without serialization.
 */
final class MemoryLogExporter implements LogExporter
{
    /**
     * @var array<LogEntry>
     */
    private array $entries = [];

    /**
     * Get all exported log entries.
     *
     * @return array<LogEntry>
     */
    public function entries() : array
    {
        return $this->entries;
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function export(array $entries) : bool
    {
        foreach ($entries as $entry) {
            $this->entries[] = $entry;
        }

        return true;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->entries = [];
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
