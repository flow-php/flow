<?php

declare(strict_types=1);

namespace Flow\Telemetry\Meter;

use Flow\Telemetry\Context\{SpanId, TraceId};

/**
 * Represents a sample measurement with trace context.
 *
 * Exemplars provide a link between aggregated metric data and the
 * distributed traces that contributed to that data. They capture
 * a representative measurement along with its trace context.
 *
 * Example:
 * ```php
 * $exemplar = new Exemplar(
 *     value: 125.5,
 *     timestamp: new \DateTimeImmutable(),
 *     traceId: $context->traceId,
 *     spanId: $activeSpanId,
 *     filteredAttributes: ['http.method' => 'GET'],
 * );
 * ```
 */
final readonly class Exemplar
{
    /**
     * @param float|int $value The recorded measurement value
     * @param \DateTimeImmutable $timestamp When the measurement was recorded
     * @param TraceId $traceId The trace ID from the active context
     * @param SpanId $spanId The span ID from the active context
     * @param array<string, bool|float|int|string> $filteredAttributes Subset of attributes for this exemplar
     */
    public function __construct(
        public int|float $value,
        public \DateTimeImmutable $timestamp,
        public TraceId $traceId,
        public SpanId $spanId,
        public array $filteredAttributes = [],
    ) {
    }

    /**
     * Create an Exemplar from a normalized array representation.
     *
     * @param array{
     *     value: float|int,
     *     timestamp: string,
     *     traceId: array{hex: string},
     *     spanId: array{hex: string},
     *     filteredAttributes: array<string, bool|float|int|string>
     * } $data Normalized Exemplar data
     */
    public static function fromArray(array $data) : self
    {
        return new self(
            $data['value'],
            new \DateTimeImmutable($data['timestamp']),
            TraceId::fromArray($data['traceId']),
            SpanId::fromArray($data['spanId']),
            $data['filteredAttributes'],
        );
    }

    /**
     * Normalize the Exemplar to an array representation for serialization.
     *
     * @return array{
     *     value: float|int,
     *     timestamp: string,
     *     traceId: array{hex: string},
     *     spanId: array{hex: string},
     *     filteredAttributes: array<string, bool|float|int|string>
     * }
     */
    public function normalize() : array
    {
        return [
            'value' => $this->value,
            'timestamp' => $this->timestamp->format('c'),
            'traceId' => $this->traceId->normalize(),
            'spanId' => $this->spanId->normalize(),
            'filteredAttributes' => $this->filteredAttributes,
        ];
    }
}
