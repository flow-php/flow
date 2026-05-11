<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use Flow\PostgreSql\Client\TypedValue;

/**
 * Formats query parameters for telemetry output.
 *
 * Converts various PHP types to string representations suitable for
 * inclusion in OpenTelemetry span attributes.
 */
final readonly class ParameterFormatter
{
    /**
     * Convert a parameter value to its string representation.
     *
     * Handles null, booleans, scalars, arrays, DateTimeInterface,
     * TypedValue objects, and objects with __toString.
     */
    public function convertToString(mixed $value): string
    {
        if ($value === null) {
            return 'NULL';
        }

        if (\is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (\is_scalar($value)) {
            return (string) $value;
        }

        if (\is_array($value)) {
            return \json_encode($value, JSON_THROW_ON_ERROR);
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('c');
        }

        if ($value instanceof TypedValue) {
            return \json_encode([
                'type' => $value->targetType->name,
                'value' => $this->convertToString($value->value),
            ], JSON_THROW_ON_ERROR);
        }

        if (\is_object($value) && \method_exists($value, '__toString')) {
            return (string) $value;
        }

        return \get_debug_type($value);
    }

    /**
     * Format a parameter value with optional length truncation.
     *
     * @param mixed $value The value to format
     * @param null|int $maxLength Maximum length (null = unlimited)
     */
    public function format(mixed $value, ?int $maxLength = null): string
    {
        $result = $this->convertToString($value);

        if ($maxLength !== null && \strlen($result) > $maxLength) {
            return \substr($result, 0, $maxLength) . '...';
        }

        return $result;
    }
}
