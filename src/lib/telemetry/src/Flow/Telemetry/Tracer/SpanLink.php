<?php

declare(strict_types=1);

namespace Flow\Telemetry\Tracer;

use Flow\Telemetry\Attributes;

/**
 * A link from one span to another.
 *
 * Links are used to associate spans that are causally related but not
 * in a direct parent-child relationship. Common use cases include:
 * - Batch operations where one span triggers multiple others
 * - Fan-in/fan-out patterns
 * - Linking to spans from previous traces
 *
 * Example usage:
 * ```php
 * $link = SpanLink::create($otherSpanContext, ['reason' => 'batch']);
 * ```
 *
 * @import-type TAttributeValue from Attributes
 * @import-type TAttributeValueMap from Attributes
 */
final readonly class SpanLink
{
    public function __construct(
        public SpanContext $context,
        public Attributes $attributes = new Attributes(),
        public int $droppedAttributeCount = 0,
    ) {}

    /**
     * Create a SpanLink with the given context and optional attributes.
     *
     * @param SpanContext $context The linked span's context
     * @param Attributes|TAttributeValueMap $attributes Link attributes
     * @param int $droppedAttributeCount Number of attributes dropped due to limits
     */
    public static function create(
        SpanContext $context,
        Attributes|array $attributes = [],
        int $droppedAttributeCount = 0,
    ): self {
        return new self(
            $context,
            $attributes instanceof Attributes ? $attributes : Attributes::create($attributes),
            $droppedAttributeCount,
        );
    }

    /**
     * Create a SpanLink from a normalized array representation.
     *
     * @param array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags?: array{byte: int}, traceState?: array{entries: array<string, string>}}, attributes: array<string, mixed>, droppedAttributeCount?: int} $data Normalized SpanLink data
     */
    public static function fromArray(array $data): self
    {
        return new self(
            SpanContext::fromArray($data['context']),
            Attributes::fromArray($data['attributes']),
            $data['droppedAttributeCount'] ?? 0,
        );
    }

    /**
     * Normalize the SpanLink to an array representation for serialization.
     *
     * @return array{context: array{traceId: array{hex: string}, spanId: array{hex: string}, parentSpanId: null|array{hex: string}, isRemote: bool, traceFlags: array{byte: int}, traceState: array{entries: array<string, string>}}, attributes: array<string, mixed>, droppedAttributeCount: int}
     */
    public function normalize(): array
    {
        return [
            'context' => $this->context->normalize(),
            'attributes' => $this->attributes->normalize(),
            'droppedAttributeCount' => $this->droppedAttributeCount,
        ];
    }
}
