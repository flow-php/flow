<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanProcessor};

/**
 * No-op span processor that discards all data.
 */
final readonly class VoidSpanProcessor implements SpanProcessor
{
    public function exporter() : SpanExporter
    {
        return new VoidSpanExporter();
    }

    public function flush() : bool
    {
        return true;
    }

    public function onEnd(Span $span) : void
    {
    }

    public function onStart(Span $span) : void
    {
    }
}
