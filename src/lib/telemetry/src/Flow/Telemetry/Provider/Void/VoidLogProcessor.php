<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Logger\{LogEntry, LogExporter, LogProcessor};

/**
 * No-op log processor that discards all data.
 */
final readonly class VoidLogProcessor implements LogProcessor
{
    public function exporter() : LogExporter
    {
        return new VoidLogExporter();
    }

    public function flush() : bool
    {
        return true;
    }

    public function process(LogEntry $entry) : void
    {
    }
}
