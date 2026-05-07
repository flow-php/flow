<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Signal\{SignalType, Signals};
use Flow\Telemetry\Tracer\Span;

/**
 * Exporter that stores telemetry batches in memory for direct access.
 *
 * Useful for testing and inspection where you need direct access
 * to exported data without serialization.
 */
final class MemoryExporter implements Exporter
{
    /**
     * @var array<LogEntry>
     */
    private array $logs = [];

    /**
     * @var array<Metric>
     */
    private array $metrics = [];

    /**
     * @var array<Span>
     */
    private array $spans = [];

    public function export(Signals $signal) : bool
    {
        match ($signal->type) {
            SignalType::LOGS => $this->logs = [...$this->logs, ...$signal->allLogs()],
            SignalType::METRICS => $this->metrics = [...$this->metrics, ...$signal->allMetrics()],
            SignalType::TRACES => $this->spans = [...$this->spans, ...$signal->allSpans()],
        };

        return true;
    }

    /**
     * @return array<LogEntry>
     */
    public function logs() : array
    {
        return $this->logs;
    }

    /**
     * @return array<Metric>
     */
    public function metrics() : array
    {
        return $this->metrics;
    }

    public function reset() : void
    {
        $this->logs = [];
        $this->metrics = [];
        $this->spans = [];
    }

    public function shutdown() : void
    {
    }

    /**
     * @return array<Span>
     */
    public function spans() : array
    {
        return $this->spans;
    }
}
