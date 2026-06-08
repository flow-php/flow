<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Meter\Metric;
use Flow\Telemetry\Meter\MetricProcessor;

use function count;

final class MetricProcessorSpy implements MetricProcessor
{
    /** @var array<Metric> */
    private array $processed = [];

    private int $flushCount = 0;

    private int $shutdownCount = 0;

    public function flush(): bool
    {
        $this->flushCount++;

        return true;
    }

    public function flushCount(): int
    {
        return $this->flushCount;
    }

    /**
     * @return array<Metric>
     */
    public function processed(): array
    {
        return $this->processed;
    }

    public function processedCount(): int
    {
        return count($this->processed);
    }

    public function process(Metric $metric): void
    {
        $this->processed[] = $metric;
    }

    public function shutdown(): void
    {
        $this->shutdownCount++;
    }

    public function shutdownCount(): int
    {
        return $this->shutdownCount;
    }
}
