<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

/**
 * Telemetry context carrier holding trace information and baggage.
 *
 * Context is the main carrier for propagating telemetry data across
 * process boundaries and through the application. It holds:
 * - A TraceId that correlates all spans in a trace
 * - An optional active SpanId for the currently executing span
 * - Baggage for arbitrary key-value data propagation
 *
 * Example usage:
 * ```php
 * $context = Context::create();
 * $context = $context->withActiveSpan(SpanId::generate());
 * echo $context->traceId->toHex();
 * ```
 */
final class Context
{
    private ?SpanId $activeSpanId = null;

    public function __construct(
        public readonly TraceId $traceId,
        public readonly Baggage $baggage = new Baggage(),
    ) {
    }

    /**
     * Create a new Context with a fresh TraceId.
     */
    public static function create() : self
    {
        return new self(TraceId::generate());
    }

    /**
     * Create a Context from a normalized array representation.
     *
     * @param array{traceId: array{hex: string}, baggage: array{entries: array<string, string>}, activeSpanId: null|array{hex: string}} $data Normalized Context data
     */
    public static function fromArray(array $data) : self
    {
        $context = new self(
            TraceId::fromArray($data['traceId']),
            Baggage::fromArray($data['baggage']),
        );

        if ($data['activeSpanId'] !== null) {
            $context->activeSpanId = SpanId::fromArray($data['activeSpanId']);
        }

        return $context;
    }

    /**
     * Create a new Context with the specified TraceId.
     *
     * @param TraceId $traceId The trace ID to use
     */
    public static function withTraceId(TraceId $traceId) : self
    {
        return new self($traceId);
    }

    /**
     * Get the currently active span ID, if any.
     *
     * @return null|SpanId The active span ID, or null if no span is active
     */
    public function activeSpanId() : ?SpanId
    {
        return $this->activeSpanId;
    }

    /**
     * Check if this is a root context (no active span).
     *
     * A root context has no active span, meaning any new span created
     * in this context would be a root span.
     */
    public function isRootContext() : bool
    {
        return $this->activeSpanId === null;
    }

    /**
     * Normalize the Context to an array representation for serialization.
     *
     * @return array{traceId: array{hex: string}, baggage: array{entries: array<string, string>}, activeSpanId: null|array{hex: string}}
     */
    public function normalize() : array
    {
        return [
            'traceId' => $this->traceId->normalize(),
            'baggage' => $this->baggage->normalize(),
            'activeSpanId' => $this->activeSpanId?->normalize(),
        ];
    }

    /**
     * Create a new Context with the specified active span.
     *
     * @param SpanId $spanId The span ID to set as active
     *
     * @return self New Context instance with the active span set
     */
    public function withActiveSpan(SpanId $spanId) : self
    {
        $context = new self($this->traceId, $this->baggage);
        $context->activeSpanId = $spanId;

        return $context;
    }

    /**
     * Create a new Context with the specified baggage.
     *
     * @param Baggage $baggage The baggage to use
     *
     * @return self New Context instance with the new baggage
     */
    public function withBaggage(Baggage $baggage) : self
    {
        $context = new self($this->traceId, $baggage);
        $context->activeSpanId = $this->activeSpanId;

        return $context;
    }

    /**
     * Create a new Context with no active span.
     *
     * @return self New Context instance with no active span
     */
    public function withoutActiveSpan() : self
    {
        return new self($this->traceId, $this->baggage);
    }
}
