<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanProcessor};

/**
 * Batches spans for efficient export.
 *
 * Collects spans in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 *
 * Example usage:
 * ```php
 * $processor = new BatchingSpanProcessor(
 *     exporter: $spanExporter,
 *     batchSize: 100,
 * );
 * ```
 */
final class BatchingSpanProcessor implements SpanProcessor
{
    /**
     * @var array<Span>
     */
    private array $buffer = [];

    public function __construct(
        private readonly SpanExporter $exporter,
        private readonly int $batchSize = 512,
    ) {
    }

    public function exporter() : SpanExporter
    {
        return $this->exporter;
    }

    public function flush() : bool
    {
        if (\count($this->buffer) === 0) {
            return true;
        }

        $spans = $this->buffer;
        $this->buffer = [];

        return $this->exporter->export($spans);
    }

    public function onEnd(Span $span) : void
    {
        $this->buffer[] = $span;

        if (\count($this->buffer) >= $this->batchSize) {
            $this->flush();
        }
    }

    public function onStart(Span $span) : void
    {
    }
}
