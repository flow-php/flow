<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor, Severity};

/**
 * Filters log entries based on minimum severity level.
 *
 * This processor wraps another LogProcessor and only passes through
 * log entries that are at or above the configured minimum severity level.
 * Entries below the threshold are silently discarded.
 *
 * Example usage:
 * ```php
 * // Only export WARN and above
 * $processor = new SeverityFilteringLogProcessor(
 *     new BatchingLogProcessor($exporter, 100),
 *     Severity::WARN,
 * );
 * ```
 */
final readonly class SeverityFilteringLogProcessor implements LogProcessor
{
    public function __construct(
        private LogProcessor $processor,
        private Severity $minimumSeverity,
    ) {
    }

    public function exporter() : LogExporter
    {
        return $this->processor->exporter();
    }

    public function flush() : bool
    {
        return $this->processor->flush();
    }

    public function process(LogEntry $entry) : void
    {
        if ($entry->record->severity->isAtLeast($this->minimumSeverity)) {
            $this->processor->process($entry);
        }
    }
}
