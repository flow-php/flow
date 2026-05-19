<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Telemetry;

use DateTimeInterface;
use Flow\PostgreSql\Client\TypedValue;

use function array_combine;
use function array_keys;
use function array_map;
use function array_slice;
use function get_debug_type;
use function is_array;
use function is_bool;
use function is_object;
use function is_scalar;
use function json_encode;
use function method_exists;
use function strlen;
use function substr;

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

        if (is_bool($value)) {
            return $value ? 'true' : 'false';
        }

        if (is_scalar($value)) {
            return (string) $value;
        }

        if (is_array($value)) {
            return json_encode($value, JSON_THROW_ON_ERROR);
        }

        if ($value instanceof DateTimeInterface) {
            return $value->format('c');
        }

        if ($value instanceof TypedValue) {
            return json_encode([
                'type' => $value->targetType->name,
                'value' => $this->convertToString($value->value),
            ], JSON_THROW_ON_ERROR);
        }

        if (is_object($value) && method_exists($value, '__toString')) {
            return (string) $value;
        }

        return get_debug_type($value);
    }

    /**
     * Format a parameter value with optional length truncation.
     */
    public function format(mixed $value, ?int $maxLength = null): string
    {
        $result = $this->convertToString($value);

        if ($maxLength !== null && strlen($result) > $maxLength) {
            return substr($result, 0, $maxLength) . '...';
        }

        return $result;
    }

    /**
     * Format a positional parameter list as DB_QUERY_PARAMETER_* span attributes.
     *
     * @param list<mixed> $parameters
     *
     * @return array<string, string>
     */
    public function formatList(array $parameters, ?int $maxParameters, ?int $maxLength): array
    {
        $slice = $maxParameters === null ? $parameters : array_slice($parameters, 0, $maxParameters);

        return array_combine(
            array_map(
                static fn(int $index): string => (
                    PostgreSqlTelemetryAttributes::DB_QUERY_PARAMETER_PREFIX . ($index + 1)
                ),
                array_keys($slice),
            ),
            array_map(fn(mixed $value): string => $this->format($value, $maxLength), $slice),
        );
    }
}
