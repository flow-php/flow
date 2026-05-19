<?php

declare(strict_types=1);

namespace Flow\Telemetry\Signal;

use Flow\Telemetry\Exception\RuntimeException;
use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Tracer\Span;

use function count;
use function sprintf;

/**
 * A typed collection of telemetry items for one signal type.
 *
 * Construct via the static factories ({@see self::logs()}, {@see self::metrics()}, {@see self::traces()}).
 * Read items via the matching accessor ({@see self::allLogs()}/{@see self::allMetrics()}/{@see self::allSpans()});
 * each accessor throws on type mismatch so the caller stays inside the correct match arm.
 */
final readonly class Signals
{
    /**
     * @param array<LogEntry> $logEntries
     * @param array<Metric> $metrics
     * @param array<Span> $spans
     */
    private function __construct(
        public SignalType $type,
        private array $logEntries = [],
        private array $metrics = [],
        private array $spans = [],
    ) {}

    /**
     * Construct a logs signal collection.
     *
     * @param array<LogEntry> $entries
     */
    public static function logs(array $entries): self
    {
        return new self(SignalType::LOGS, logEntries: $entries);
    }

    /**
     * Construct a metrics signal collection.
     *
     * @param array<Metric> $metrics
     */
    public static function metrics(array $metrics): self
    {
        return new self(SignalType::METRICS, metrics: $metrics);
    }

    /**
     * Construct a traces signal collection.
     *
     * @param array<Span> $spans
     */
    public static function traces(array $spans): self
    {
        return new self(SignalType::TRACES, spans: $spans);
    }

    /**
     * Read the log entries from a logs signal collection.
     *
     * @throws RuntimeException if the signal does not carry logs
     *
     * @return array<LogEntry>
     */
    public function allLogs(): array
    {
        if ($this->type !== SignalType::LOGS) {
            throw new RuntimeException(sprintf(
                'Signals collection of type %s does not carry log entries',
                $this->type->name,
            ));
        }

        return $this->logEntries;
    }

    /**
     * Read the metrics from a metrics signal collection.
     *
     * @throws RuntimeException if the signal does not carry metrics
     *
     * @return array<Metric>
     */
    public function allMetrics(): array
    {
        if ($this->type !== SignalType::METRICS) {
            throw new RuntimeException(sprintf(
                'Signals collection of type %s does not carry metrics',
                $this->type->name,
            ));
        }

        return $this->metrics;
    }

    /**
     * Read the spans from a traces signal collection.
     *
     * @throws RuntimeException if the signal does not carry spans
     *
     * @return array<Span>
     */
    public function allSpans(): array
    {
        if ($this->type !== SignalType::TRACES) {
            throw new RuntimeException(sprintf(
                'Signals collection of type %s does not carry spans',
                $this->type->name,
            ));
        }

        return $this->spans;
    }

    public function count(): int
    {
        return match ($this->type) {
            SignalType::LOGS => count($this->logEntries),
            SignalType::METRICS => count($this->metrics),
            SignalType::TRACES => count($this->spans),
        };
    }

    public function isEmpty(): bool
    {
        return $this->count() === 0;
    }
}
