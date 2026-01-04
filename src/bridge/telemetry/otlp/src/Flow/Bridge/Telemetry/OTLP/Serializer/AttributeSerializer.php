<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\Attributes;

/**
 * Serializes telemetry attributes to OTLP JSON format.
 *
 * OTLP attributes use a typed value wrapper format where each attribute
 * has a key and a value object with a type-specific field.
 *
 * @see https://opentelemetry.io/docs/specs/otlp/#otlphttp-request
 */
final class AttributeSerializer
{
    /**
     * Serialize attributes to OTLP format.
     *
     * @return array<array{key: string, value: array<string, mixed>}>
     */
    public function serialize(Attributes $attributes) : array
    {
        $result = [];

        foreach ($attributes->normalize() as $key => $value) {
            $result[] = [
                'key' => $key,
                'value' => $this->serializeValue($value),
            ];
        }

        return $result;
    }

    /**
     * Serialize a single attribute value to OTLP format.
     *
     * @param array<bool|float|int|string>|bool|float|int|string $value
     *
     * @return array<string, mixed>
     */
    public function serializeValue(string|int|float|bool|array $value) : array
    {
        if (\is_string($value)) {
            return ['stringValue' => $value];
        }

        if (\is_int($value)) {
            return ['intValue' => (string) $value];
        }

        if (\is_float($value)) {
            return ['doubleValue' => $value];
        }

        if (\is_bool($value)) {
            return ['boolValue' => $value];
        }

        return $this->serializeArray($value);
    }

    /**
     * Serialize an array attribute value to OTLP format.
     *
     * @param array<bool|float|int|string> $values
     *
     * @return array{arrayValue: array{values: array<array<string, mixed>>}}
     */
    private function serializeArray(array $values) : array
    {
        $serialized = [];

        foreach ($values as $value) {
            $serialized[] = $this->serializeValue($value);
        }

        return [
            'arrayValue' => [
                'values' => $serialized,
            ],
        ];
    }
}
