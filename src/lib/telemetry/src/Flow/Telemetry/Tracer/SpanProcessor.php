<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

/**
 * Interface for processing spans when they start and end.
 *
 * Implementations may collect spans for batching, export them immediately,
 * or perform other processing like filtering or sampling.
 */
interface SpanProcessor
{
    /**
     * Export all pending spans and return success status.
     *
     * Forces immediate export of any buffered spans. Returns true
     * if all spans were successfully exported.
     */
    public function flush(): bool;

    /**
     * Called when a span ends.
     *
     * This is invoked synchronously when the span completes. The span
     * is fully populated at this point (end time, status, all attributes).
     */
    public function onEnd(Span $span): void;

    /**
     * Called when a span starts.
     *
     * This is invoked synchronously when the span begins. Implementations
     * should avoid blocking operations in this method.
     */
    public function onStart(Span $span): void;

    /**
     * Shutdown the processor.
     *
     * Implementations SHOULD flush() pending data before delegating shutdown
     * to the underlying exporter. MUST be idempotent and MUST NOT throw.
     */
    public function shutdown(): void;
}
