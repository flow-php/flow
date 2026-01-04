<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanProcessor};

/**
 * Exports each span immediately when it ends.
 *
 * Unlike BatchingSpanProcessor, this processor exports spans synchronously
 * one at a time. This is useful for debugging and development where
 * immediate visibility of spans is more important than performance.
 *
 * Example usage:
 * ```php
 * $processor = new PassThroughSpanProcessor($spanExporter);
 * ```
 */
final readonly class PassThroughSpanProcessor implements SpanProcessor
{
    public function __construct(
        private SpanExporter $exporter,
    ) {
    }

    public function exporter() : SpanExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        return true;
    }

    public function onEnd(Span $span) : void
    {
        $this->exporter->export([$span]);
    }

    public function onStart(Span $span) : void
    {
    }
}
