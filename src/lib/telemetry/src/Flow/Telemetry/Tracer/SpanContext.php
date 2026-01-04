<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Context\{SpanId, TraceFlags, TraceId, TraceState};

/**
 * Immutable identification of a span within a trace.
 *
 * SpanContext carries the identifying information that makes a span unique
 * within a trace: the trace ID, span ID, and optional parent span ID.
 * It also tracks whether the span originated from a remote process,
 * and includes trace flags and vendor-specific trace state.
 *
 * Example usage:
 * ```php
 * $context = SpanContext::create(
 *     TraceId::generate(),
 *     SpanId::generate(),
 * );
 * echo $context->traceId->toHex();
 * echo $context->traceFlags->isSampled() ? 'sampled' : 'not sampled';
 * ```
 */
final readonly class SpanContext
{
    public TraceFlags $traceFlags;

    public TraceState $traceState;

    public function __construct(
        public TraceId $traceId,
        public SpanId $spanId,
        public ?SpanId $parentSpanId = null,
        public bool $isRemote = false,
        ?TraceFlags $traceFlags = null,
        ?TraceState $traceState = null,
    ) {
        $this->traceFlags = $traceFlags ?? TraceFlags::default();
        $this->traceState = $traceState ?? TraceState::empty();
    }

    /**
     * Create a local SpanContext.
     */
    public static function create(
        TraceId $traceId,
        SpanId $spanId,
        ?SpanId $parentSpanId = null,
        ?TraceFlags $traceFlags = null,
        ?TraceState $traceState = null,
    ) : self {
        return new self(
            $traceId,
            $spanId,
            $parentSpanId,
            false,
            $traceFlags ?? TraceFlags::default(),
            $traceState ?? TraceState::empty(),
        );
    }

    /**
     * Create a remote SpanContext (received from another process).
     */
    public static function createRemote(
        TraceId $traceId,
        SpanId $spanId,
        ?SpanId $parentSpanId = null,
        ?TraceFlags $traceFlags = null,
        ?TraceState $traceState = null,
    ) : self {
        return new self(
            $traceId,
            $spanId,
            $parentSpanId,
            true,
            $traceFlags ?? TraceFlags::default(),
            $traceState ?? TraceState::empty(),
        );
    }

    /**
     * Create a SpanContext from a normalized array representation.
     *
     * @param array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags?: array{byte: int}, traceState?: array{entries: array<string, string>}} $data Normalized SpanContext data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            TraceId::fromArray($data['traceId']),
            SpanId::fromArray($data['spanId']),
            $data['parentSpanId'] !== null ? SpanId::fromArray($data['parentSpanId']) : null,
            $data['isRemote'],
            isset($data['traceFlags']) ? TraceFlags::fromArray($data['traceFlags']) : TraceFlags::default(),
            isset($data['traceState']) ? TraceState::fromArray($data['traceState']) : TraceState::empty(),
        );
    }

    /**
     * Check if this is a root span (no parent).
     */
    public function isRoot() : bool
    {
        return $this->parentSpanId === null;
    }

    /**
     * Check if this SpanContext has valid trace and span IDs.
     *
     * A SpanContext is valid when both the trace ID and span ID are non-zero.
     * Note: SpanId and TraceId already reject all-zero values at construction,
     * so any successfully created SpanContext is valid by construction.
     */
    public function isValid() : bool
    {
        return true;
    }

    /**
     * Normalize the SpanContext to an array representation for serialization.
     *
     * @return array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}}
     */
    public function normalize() : array
    {
        return [
            'traceId' => $this->traceId->normalize(),
            'spanId' => $this->spanId->normalize(),
            'parentSpanId' => $this->parentSpanId?->normalize(),
            'isRemote' => $this->isRemote,
            'traceFlags' => $this->traceFlags->normalize(),
            'traceState' => $this->traceState->normalize(),
        ];
    }

    /**
     * Create a new SpanContext with the specified trace flags.
     */
    public function withTraceFlags(TraceFlags $traceFlags) : self
    {
        return new self(
            $this->traceId,
            $this->spanId,
            $this->parentSpanId,
            $this->isRemote,
            $traceFlags,
            $this->traceState,
        );
    }

    /**
     * Create a new SpanContext with the specified trace state.
     */
    public function withTraceState(TraceState $traceState) : self
    {
        return new self(
            $this->traceId,
            $this->spanId,
            $this->parentSpanId,
            $this->isRemote,
            $this->traceFlags,
            $traceState,
        );
    }
}
