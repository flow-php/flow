<?php

declare(strict_types=1);

namespace Flow\Telemetry\Transport;

use Flow\Telemetry\Signal\Signals;

/**
 * A no-op transport for exporters that don't need actual transport.
 *
 * Used by console, memory, and void exporters to satisfy the requirement
 * that all exporters must return at least one transport.
 */
final class VoidTransport implements Transport
{
    public function send(Signals $signal) : void
    {
    }

    public function shutdown() : void
    {
    }
}
