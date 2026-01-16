<?php

declare(strict_types=1);

namespace Flow\Telemetry\Provider\Memory;

use Flow\Telemetry\Tracer\{Span, SpanExporter, SpanProcessor};

/**
 * Processor that stores spans in memory and exports via configured exporter.
 */
final class MemorySpanProcessor implements SpanProcessor
{
    /**
     * @var array<string, array<Span>>
     */
    private array $endedSpansByTraceId = [];

    /**
     * @var array<string, array<Span>>
     */
    private array $startedSpansByTraceId = [];

    public function __construct(
        private readonly SpanExporter $spanExporter,
    ) {
    }

    /**
     * Get all spans that have ended.
     *
     * @return array<Span>
     */
    public function endedSpans() : array
    {
        return \array_merge(...\array_values($this->endedSpansByTraceId));
    }

    /**
     * Get all ended spans for a specific trace.
     *
     * @return array<Span>
     */
    public function endedSpansForTrace(string $traceId) : array
    {
        return $this->endedSpansByTraceId[$traceId] ?? [];
    }

    public function exporter() : SpanExporter
    {
        return $this->spanExporter;
    }

    public function flush() : bool
    {
        $spans = $this->endedSpans();

        if (\count($spans) === 0) {
            return true;
        }

        return $this->spanExporter->export($spans);
    }

    public function onEnd(Span $span) : void
    {
        $traceId = $span->context()->traceId->toHex();

        if (!isset($this->endedSpansByTraceId[$traceId])) {
            $this->endedSpansByTraceId[$traceId] = [];
        }

        $this->endedSpansByTraceId[$traceId][] = $span;
    }

    public function onStart(Span $span) : void
    {
        $traceId = $span->context()->traceId->toHex();

        if (!isset($this->startedSpansByTraceId[$traceId])) {
            $this->startedSpansByTraceId[$traceId] = [];
        }

        $this->startedSpansByTraceId[$traceId][] = $span;
    }

    /**
     * Reset all stored data.
     */
    public function reset() : void
    {
        $this->startedSpansByTraceId = [];
        $this->endedSpansByTraceId = [];
    }

    /**
     * Get all spans that have started.
     *
     * @return array<Span>
     */
    public function startedSpans() : array
    {
        return \array_merge(...\array_values($this->startedSpansByTraceId));
    }

    /**
     * Get all started spans for a specific trace.
     *
     * @return array<Span>
     */
    public function startedSpansForTrace(string $traceId) : array
    {
        return $this->startedSpansByTraceId[$traceId] ?? [];
    }

    /**
     * Get all trace IDs that have spans recorded.
     *
     * @return array<string>
     */
    public function traceIds() : array
    {
        return \array_values(\array_unique(\array_merge(
            \array_keys($this->startedSpansByTraceId),
            \array_keys($this->endedSpansByTraceId),
        )));
    }
}
