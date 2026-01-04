<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Logger\{LogEntry, LogExporter};
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * No-op log exporter that discards all data.
 */
final readonly class VoidLogExporter implements LogExporter
{
    /**
     * @param array<LogEntry> $entries
     */
    public function export(array $entries) : bool
    {
        return true;
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [new VoidTransport()];
    }
}
