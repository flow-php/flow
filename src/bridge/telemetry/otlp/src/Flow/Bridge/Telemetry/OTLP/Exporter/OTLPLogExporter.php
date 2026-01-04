<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Exporter;

use Flow\Telemetry\Logger\{LogEntry, LogExporter};
use Flow\Telemetry\Transport\Transport;

/**
 * Exports log records to OTLP endpoint.
 *
 * Example usage:
 * ```php
 * $exporter = new OTLPLogExporter(
 *     transport: $httpTransport,
 * );
 *
 * $exporter->export($entries);
 * ```
 */
final readonly class OTLPLogExporter implements LogExporter
{
    public function __construct(
        private Transport $transport,
    ) {
    }

    /**
     * @param array<LogEntry> $entries
     */
    public function export(array $entries) : bool
    {
        if (\count($entries) === 0) {
            return true;
        }

        try {
            $this->transport->sendLogs($entries);

            return true;
        } catch (\Throwable) {
            return false;
        }
    }

    /**
     * @return array<Transport>
     */
    public function transports() : array
    {
        return [$this->transport];
    }
}
