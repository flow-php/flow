<?php

declare(strict_types=1);

namespace Flow\Telemetry\Logger\Processor;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor};

/**
 * Exports each log record immediately when processed.
 *
 * Unlike BatchingLogProcessor, this processor exports log records synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of logs is more important than performance.
 *
 * Example usage:
 * ```php
 * $processor = new PassThroughLogProcessor($logExporter);
 * ```
 */
final readonly class PassThroughLogProcessor implements LogProcessor
{
    public function __construct(
        private LogExporter $exporter,
    ) {
    }

    public function exporter() : LogExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        return true;
    }

    public function process(LogEntry $entry) : void
    {
        $this->exporter->export([$entry]);
    }
}
