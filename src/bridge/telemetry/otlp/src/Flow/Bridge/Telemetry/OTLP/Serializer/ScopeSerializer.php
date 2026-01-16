<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\InstrumentationScope;

/**
 * Serializes InstrumentationScope to OTLP JSON format.
 *
 * OTLP scope identifies the instrumentation library/component producing telemetry.
 *
 * @see https://opentelemetry.io/docs/specs/otel/glossary/#instrumentation-scope
 */
final readonly class ScopeSerializer
{
    public function __construct(
        private AttributeSerializer $attributeSerializer = new AttributeSerializer(),
    ) {
    }

    /**
     * Serialize an InstrumentationScope to OTLP format.
     *
     * @return array{name: string, version?: string, attributes?: array<array{key: string, value: array<string, mixed>}>}
     */
    public function serialize(InstrumentationScope $scope) : array
    {
        $result = [
            'name' => $scope->name,
        ];

        if ($scope->version !== 'unknown') {
            $result['version'] = $scope->version;
        }

        if ($scope->attributes->count() > 0) {
            $result['attributes'] = $this->attributeSerializer->serialize($scope->attributes);
        }

        return $result;
    }
}
