<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\ErrorHandler\{ErrorHandler, ErrorLogHandler};
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\{LogEntry, LogProcessor, Severity};
use Flow\Telemetry\Signal\Signals;

/**
 * Processor that stores log entries in memory and exports via configured exporter.
 */
final class MemoryLogProcessor implements LogProcessor
{
    /**
     * @var array<LogEntry>
     */
    private array $entries = [];

    private bool $isShutdown = false;

    public function __construct(
        private readonly Exporter $logExporter,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
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

    public function flush() : bool
    {
        if (\count($this->entries) === 0) {
            return true;
        }

        try {
            return $this->logExporter->export(Signals::logs($this->entries));
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    public function process(LogEntry $entry) : void
    {
        $this->entries[] = $entry;
    }

    public function reset() : void
    {
        $this->entries = [];
    }

    public function shutdown() : void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $this->flush();

        try {
            $this->logExporter->shutdown();
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
