<?php

declare(strict_types=1);

namespace Flow\Bridge\Telemetry\OTLP\Serializer;

use Flow\Telemetry\Attributes;

use function array_is_list;
use function array_keys;
use function array_map;
use function get_debug_type;
use function is_array;
use function is_bool;
use function is_float;
use function is_int;
use function is_string;

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
    public function serialize(Attributes $attributes): array
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
     * @return array<string, mixed>
     */
    public function serializeValue(mixed $value): array
    {
        if (is_string($value)) {
            return ['stringValue' => $value];
        }

        if (is_int($value)) {
            return ['intValue' => (string) $value];
        }

        if (is_float($value)) {
            return ['doubleValue' => $value];
        }

        if (is_bool($value)) {
            return ['boolValue' => $value];
        }

        if (is_array($value)) {
            if (array_is_list($value)) {
                return $this->serializeArrayValue($value);
            }

            return $this->serializeKvlistValue($value);
        }

        return ['stringValue' => get_debug_type($value)];
    }

    /**
     * @param list<mixed> $values
     *
     * @return array{arrayValue: array{values: list<array<string, mixed>>}}
     */
    private function serializeArrayValue(array $values): array
    {
        return [
            'arrayValue' => [
                'values' => array_map($this->serializeValue(...), $values),
            ],
        ];
    }

    /**
     * @param array<array-key, mixed> $values
     *
     * @return array{kvlistValue: array{values: list<array{key: string, value: array<string, mixed>}>}}
     */
    private function serializeKvlistValue(array $values): array
    {
        return [
            'kvlistValue' => [
                'values' => array_map(
                    fn(int|string $key, mixed $value): array => [
                        'key' => (string) $key,
                        'value' => $this->serializeValue($value),
                    ],
                    array_keys($values),
                    $values,
                ),
            ],
        ];
    }
}
