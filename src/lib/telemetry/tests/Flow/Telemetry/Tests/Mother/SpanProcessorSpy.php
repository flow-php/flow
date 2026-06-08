<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

use function count;

final class SpanProcessorSpy implements SpanProcessor
{
    /** @var array<Span> */
    private array $ended = [];

    private int $flushCount = 0;

    private int $shutdownCount = 0;

    /** @var array<Span> */
    private array $started = [];

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
     * @return array<Span>
     */
    public function ended(): array
    {
        return $this->ended;
    }

    public function endedCount(): int
    {
        return count($this->ended);
    }

    public function onEnd(Span $span): void
    {
        $this->ended[] = $span;
    }

    public function onStart(Span $span): void
    {
        $this->started[] = $span;
    }

    public function shutdown(): void
    {
        $this->shutdownCount++;
    }

    public function shutdownCount(): int
    {
        return $this->shutdownCount;
    }

    /**
     * @return array<Span>
     */
    public function started(): array
    {
        return $this->started;
    }

    public function startedCount(): int
    {
        return count($this->started);
    }
}
