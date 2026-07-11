<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Conditional;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;

/**
 * Gates a wrapped exporter behind a runtime flag. When disabled, batches are
 * dropped without reaching the wrapped exporter, while callers still observe a
 * successful export. Enables toggling export on or off via configuration
 * (including environment variables) without rewiring the container.
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
