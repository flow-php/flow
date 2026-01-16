<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor, Severity};

/**
 * Processor that stores log entries in memory and exports via configured exporter.
 */
final class MemoryLogProcessor implements LogProcessor
{
    /**
     * @var array<LogEntry>
     */
    private array $entries = [];

    public function __construct(
        private readonly LogExporter $logExporter,
    ) {
    }

    /**
     * Get the total number of recorded log entries.
     */
    public function countLogs() : int
    {
        return \count($this->entries);
    }

    /**
     * Get all recorded log entries.
     *
     * @return array<LogEntry>
     */
    public function entries() : array
    {
        return $this->entries;
    }

    /**
     * Filter log entries by body substring.
     *
     * @return array<LogEntry>
     */
    public function entriesContaining(string $substring) : array
    {
        return \array_values(\array_filter(
            $this->entries,
            static fn (LogEntry $entry) : bool => \str_contains($entry->record->body, $substring)
        ));
    }

    /**
     * Filter log entries by severity.
     *
     * @return array<LogEntry>
     */
    public function entriesWithSeverity(Severity $severity) : array
    {
        return \array_values(\array_filter(
            $this->entries,
            static fn (LogEntry $entry) : bool => $entry->record->severity === $severity
        ));
    }

    public function exporter() : LogExporter
    {
        return $this->logExporter;
    }

    public function flush() : bool
    {
        if (\count($this->entries) === 0) {
            return true;
        }

        return $this->logExporter->export($this->entries);
    }

    public function process(LogEntry $entry) : void
    {
        $this->entries[] = $entry;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->entries = [];
    }
}
