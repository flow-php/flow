<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Transport\{Transport, VoidTransport};

/**
 * No-op exporter that discards all data.
 */
final readonly class VoidExporter implements Exporter
{
    public function export(Signals $signal) : bool
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
