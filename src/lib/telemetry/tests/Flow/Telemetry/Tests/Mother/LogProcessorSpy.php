<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Logger\LogEntry;
use Flow\Telemetry\Logger\LogSink;

use function count;

final class LogProcessorSpy implements LogSink
{
    /** @var array<LogEntry> */
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
     * @return array<LogEntry>
     */
    public function processed(): array
    {
        return $this->processed;
    }

    public function processedCount(): int
    {
        return count($this->processed);
    }

    public function process(LogEntry $entry): void
    {
        $this->processed[] = $entry;
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
