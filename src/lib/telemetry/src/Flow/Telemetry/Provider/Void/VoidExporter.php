<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;

/**
 * No-op exporter that discards all data.
 */
final readonly class VoidExporter implements Exporter
{
    public function export(Signals $signal) : bool
    {
        return true;
    }

    public function shutdown() : void
    {
    }
}
