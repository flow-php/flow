<?php

declare(strict_types=1);

namespace Flow\Telemetry\Context;

use Flow\Telemetry\Tracer\SpanContext;

/**
 * Telemetry context carrier holding the active span and baggage.
 *
 * Follows the OpenTelemetry context model: a Context either carries the currently active span (its
 * SpanContext) — making the next span its child within the same trace — or carries none, making the next
 * span a new trace root. Trace id is always derived from the active span; it is never stored independently.
 *
 * Example usage:
 * ```php
 * $context = Context::root();
 * $context = $context->withActiveSpan($span->context());
 * echo $context->traceId()?->toHex();
 * ```
 */
final class Context
{
    public function __construct(
        public readonly ?SpanContext $activeSpan = null,
        public readonly Baggage $baggage = new Baggage(),
    ) {}

    public static function root(): self
    {
        return new self();
    }

    /**
     * @param array{activeSpan: null|array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags?: array{byte: int}, traceState?: array{entries: array<string, string>}}, baggage: array{entries: array<string, string>}} $data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            $data['activeSpan'] !== null ? SpanContext::fromArray($data['activeSpan']) : null,
            Baggage::fromArray($data['baggage']),
        );
    }

    public function activeSpan(): ?SpanContext
    {
        return $this->activeSpan;
    }

    public function activeSpanId(): ?SpanId
    {
        return $this->activeSpan?->spanId;
    }

    public function traceId(): ?TraceId
    {
        return $this->activeSpan?->traceId;
    }

    public function isRootContext(): bool
    {
        return $this->activeSpan === null;
    }

    /**
     * @return array{activeSpan: null|array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}}, baggage: array{entries: array<string, string>}}
     */
    public function normalize(): array
    {
        return [
            'activeSpan' => $this->activeSpan?->normalize(),
            'baggage' => $this->baggage->normalize(),
        ];
    }

    public function withActiveSpan(SpanContext $span): self
    {
        return new self($span, $this->baggage);
    }

    public function withBaggage(Baggage $baggage): self
    {
        return new self($this->activeSpan, $baggage);
    }

    public function withoutActiveSpan(): self
    {
        return new self(null, $this->baggage);
    }
}
