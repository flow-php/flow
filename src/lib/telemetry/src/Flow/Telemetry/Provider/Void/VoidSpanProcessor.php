<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Tracer\Span;
use Flow\Telemetry\Tracer\SpanProcessor;

/**
 * No-op span processor that discards all data.
 */
final readonly class VoidSpanProcessor implements SpanProcessor
{
    public function flush(): bool
    {
        return true;
    }

    public function onEnd(Span $span): void {}

    public function onStart(Span $span): void {}

    public function shutdown(): void {}
}
