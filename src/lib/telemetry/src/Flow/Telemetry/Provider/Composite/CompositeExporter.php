<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Composite;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;

/**
 * Fans every exported batch out to multiple exporters.
 */
final readonly class CompositeExporter implements Exporter
{
    /**
     * @param array<Exporter> $exporters
     */
    public function __construct(
        private array $exporters,
    ) {}

    public function export(Signals $signal): bool
    {
        $ok = true;

        foreach ($this->exporters as $exporter) {
            $ok = $exporter->export($signal) && $ok;
        }

        return $ok;
    }

    public function shutdown(): void
    {
        foreach ($this->exporters as $exporter) {
            $exporter->shutdown();
        }
    }
}
