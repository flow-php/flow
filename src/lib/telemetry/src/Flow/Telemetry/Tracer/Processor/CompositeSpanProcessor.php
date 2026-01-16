<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanProcessor};

/**
 * Forwards spans to multiple processors.
 *
 * This is useful when you need to:
 * - Send spans to multiple backends (e.g., both console and OTLP)
 * - Combine batching with memory storage for testing
 * - Add custom processing alongside export
 *
 * Example usage:
 * ```php
 * $processor = new CompositeSpanProcessor([
 *     new BatchingSpanProcessor($otlpExporter),
 *     new MemorySpanProcessor(),
 * ]);
 * ```
 */
final readonly class CompositeSpanProcessor implements SpanProcessor
{
    /**
     * @param array<SpanProcessor> $processors
     */
    public function __construct(
        private array $processors,
    ) {
    }

    public function exporter() : SpanExporter
    {
        if (\count($this->processors) === 0) {
            throw new \RuntimeException('CompositeSpanProcessor has no processors');
        }

        return $this->processors[0]->exporter();
    }

    public function flush() : bool
    {
        $success = true;

        foreach ($this->processors as $processor) {
            if (!$processor->flush()) {
                $success = false;
            }
        }

        return $success;
    }

    public function onEnd(Span $span) : void
    {
        foreach ($this->processors as $processor) {
            $processor->onEnd($span);
        }
    }

    public function onStart(Span $span) : void
    {
        foreach ($this->processors as $processor) {
            $processor->onStart($span);
        }
    }

    /**
     * Get all processors in this composite.
     *
     * @return array<SpanProcessor>
     */
    public function processors() : array
    {
        return $this->processors;
    }
}
