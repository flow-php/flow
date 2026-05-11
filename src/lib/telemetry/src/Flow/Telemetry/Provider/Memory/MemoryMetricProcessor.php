<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\ErrorHandler\ErrorHandler;
use Flow\Telemetry\ErrorHandler\ErrorLogHandler;
use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;
use Flow\Telemetry\Meter\MetricType;
use Flow\Telemetry\Signal\Signals;

/**
 * Processor that stores metrics in memory and exports via configured exporter.
 */
final class MemoryMetricProcessor implements MetricProcessor
{
    private bool $isShutdown = false;

    /**
     * @var array<Metric>
     */
    private array $metrics = [];

    public function __construct(
        private readonly Exporter $metricExporter,
        private readonly ErrorHandler $errorHandler = new ErrorLogHandler(),
    ) {}

    public function countMetrics(): int
    {
        return \count($this->metrics);
    }

    public function flush(): bool
    {
        if (\count($this->metrics) === 0) {
            return true;
        }

        try {
            return $this->metricExporter->export(Signals::metrics($this->metrics));
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);

            return false;
        }
    }

    /**
     * @return array<Metric>
     */
    public function metrics(): array
    {
        return $this->metrics;
    }

    /**
     * @return array<Metric>
     */
    public function metricsOfType(MetricType $type): array
    {
        return \array_values(\array_filter($this->metrics, static fn(Metric $metric): bool => $metric->type === $type));
    }

    /**
     * @return array<Metric>
     */
    public function metricsWithName(string $name): array
    {
        return \array_values(\array_filter($this->metrics, static fn(Metric $metric): bool => $metric->name === $name));
    }

    public function process(Metric $metric): void
    {
        $this->metrics[] = $metric;
    }

    public function reset(): void
    {
        $this->metrics = [];
    }

    public function shutdown(): void
    {
        if ($this->isShutdown) {
            return;
        }

        $this->isShutdown = true;

        $this->flush();

        try {
            $this->metricExporter->shutdown();
        } catch (\Throwable $e) {
            $this->errorHandler->handle($e);
        }
    }
}
