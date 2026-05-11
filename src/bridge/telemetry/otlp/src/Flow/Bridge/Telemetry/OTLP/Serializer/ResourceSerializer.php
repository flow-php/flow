<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\Resource;

/**
 * Serializes telemetry Resource to OTLP JSON format.
 *
 * OTLP resources contain attributes describing the entity producing telemetry.
 *
 * @see https://opentelemetry.io/docs/specs/otel/resource/sdk/
 */
final readonly class ResourceSerializer
{
    public function __construct(
        private AttributeSerializer $attributeSerializer = new AttributeSerializer(),
    ) {}

    /**
     * Serialize a Resource to OTLP format.
     *
     * @return array{attributes: array<array{key: string, value: array<string, mixed>}>}
     */
    public function serialize(Resource $resource): array
    {
        return [
            'attributes' => $this->attributeSerializer->serialize($resource->attributes),
        ];
    }
}
