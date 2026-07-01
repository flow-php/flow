<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Signal\SignalType;
use Flow\Telemetry\Tracer\Span;
use InvalidArgumentException;

use function array_slice;
use function count;
use function sprintf;

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

    /**
     * @param null|int $maxEntriesPerSignal maximum number of entries retained per signal type; once exceeded the
     *                                      oldest entries are dropped. Null (default) keeps everything, which is
     *                                      unbounded and unsafe in long-running processes that never reset().
     */
    public function __construct(
        private readonly ?int $maxEntriesPerSignal = null,
    ) {
        if ($maxEntriesPerSignal !== null && $maxEntriesPerSignal < 1) {
            throw new InvalidArgumentException(sprintf(
                'MemoryExporter maxEntriesPerSignal must be a positive integer, got %d',
                $maxEntriesPerSignal,
            ));
        }
    }

    public function export(Signals $signal): bool
    {
        match ($signal->type) {
            SignalType::LOGS => $this->logs = $this->cap([...$this->logs, ...$signal->allLogs()]),
            SignalType::METRICS => $this->metrics = $this->cap([...$this->metrics, ...$signal->allMetrics()]),
            SignalType::TRACES => $this->spans = $this->cap([...$this->spans, ...$signal->allSpans()]),
        };

        return true;
    }

    /**
     * @return array<LogEntry>
     */
    public function logs(): array
    {
        return $this->logs;
    }

    /**
     * @return array<Metric>
     */
    public function metrics(): array
    {
        return $this->metrics;
    }

    public function reset(): void
    {
        $this->logs = [];
        $this->metrics = [];
        $this->spans = [];
    }

    public function shutdown(): void {}

    /**
     * @return array<Span>
     */
    public function spans(): array
    {
        return $this->spans;
    }

    /**
     * @template T
     *
     * @param array<T> $entries
     *
     * @return array<T>
     */
    private function cap(array $entries): array
    {
        if ($this->maxEntriesPerSignal === null || count($entries) <= $this->maxEntriesPerSignal) {
            return $entries;
        }

        return array_slice($entries, -$this->maxEntriesPerSignal);
    }
}
