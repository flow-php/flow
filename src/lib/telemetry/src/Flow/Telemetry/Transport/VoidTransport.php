<?php

declare(strict_types=1);

namespace Flow\Telemetry\Transport;

/**
 * A no-op transport for exporters that don't need actual transport.
 *
 * Used by console, memory, and void exporters to satisfy the requirement
 * that all exporters must return at least one transport.
 */
final class VoidTransport implements Transport
{
    public function sendLogs(array $entries) : void
    {
    }

    public function sendMetrics(array $metrics) : void
    {
    }

    public function sendSpans(array $spans) : void
    {
    }

    public function shutdown() : void
    {
    }
}
