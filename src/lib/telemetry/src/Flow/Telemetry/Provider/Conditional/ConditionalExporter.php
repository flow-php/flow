<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Conditional;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;

/**
 * When disabled, batches are dropped but export still reports success.
 */
final readonly class ConditionalExporter implements Exporter
{
    public function __construct(
        private bool $enabled,
        private Exporter $exporter,
    ) {}

    public function export(Signals $signal): bool
    {
        if (!$this->enabled) {
            return true;
        }

        return $this->exporter->export($signal);
    }

    public function shutdown(): void
    {
        $this->exporter->shutdown();
    }
}
