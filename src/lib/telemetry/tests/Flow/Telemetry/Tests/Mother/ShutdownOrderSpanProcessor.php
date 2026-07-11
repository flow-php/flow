<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tests\Mother;

use ArrayObject;
use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

/**
 * Appends its name to a shared log on shutdown, so tests can assert the order
 * in which multiple telemetry instances are shut down.
 */
final class ShutdownOrderSpanProcessor implements SpanProcessor
{
    /**
     * @param ArrayObject<int, string> $shutdownLog
     */
    public function __construct(
        private readonly string $name,
        private readonly ArrayObject $shutdownLog,
    ) {}

    public function flush(): bool
    {
        return true;
    }

    public function onEnd(Span $span): void {}

    public function onStart(Span $span): void {}

    public function shutdown(): void
    {
        $this->shutdownLog->append($this->name);
    }
}
