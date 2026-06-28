<?php

declare(strict_types=1);

namespace Flow\Bridge\Symfony\TelemetryBundle\Tests\Fixtures\Telemetry;

use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

final class SpySpanProcessor implements SpanProcessor
{
    public int $flushCount = 0;

    public int $shutdownCount = 0;

    public function flush(): bool
    {
        $this->flushCount++;

        return true;
    }

    public function onEnd(Span $span): void {}

    public function onStart(Span $span): void {}

    public function shutdown(): void
    {
        $this->shutdownCount++;
    }
}
