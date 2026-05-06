<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Void;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Tracer\{Span, SpanProcessor};

/**
 * No-op span processor that discards all data.
 */
final readonly class VoidSpanProcessor implements SpanProcessor
{
    public function exporter() : Exporter
    {
        return new VoidExporter();
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
