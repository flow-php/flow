<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer\Processor;

use Flow\Telemetry\Exporter\Exporter;
use Flow\Telemetry\Signal\Signals;
use Flow\Telemetry\Tracer\{Span, SpanProcessor};

/**
 * Batches spans for efficient export.
 *
 * Collects spans in memory and exports them in batches when:
 * - The batch size limit is reached
 * - flush() is explicitly called
 * - the system is shutting down
 */
final class BatchingSpanProcessor implements SpanProcessor
{
    /**
     * @var array<Span>
     */
    private array $buffer = [];

    public function __construct(
        private readonly Exporter $exporter,
        private readonly int $batchSize = 512,
    ) {
    }

    public function exporter() : Exporter
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

        return $this->exporter->export(Signals::traces($spans));
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
