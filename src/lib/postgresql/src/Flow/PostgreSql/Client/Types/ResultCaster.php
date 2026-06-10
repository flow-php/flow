<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\PostgreSql\Client\Exception\ValueConversionException;

use function floatval;
use function hex2bin;
use function substr;

use const INF;
use const NAN;
use const PHP_INT_SIZE;

/**
 * Casts PostgreSQL result values to unambiguous PHP types.
 *
 * Uses string type names from pg_field_type() for portability (OIDs can vary with extensions).
 *
 * Only converts types that have a clear, unambiguous mapping:
 * - bool → bool
 * - int2, int4 → int
 * - int8 → int (on 64-bit) or string (on 32-bit to avoid overflow)
 * - float4, float8 → float (including Infinity, -Infinity, NaN)
 * - bytea → string (decoded binary)
 * - timestamp → string with a +00:00 offset appended (values are stored in UTC)
 *
 * All other types remain as strings for higher layers to interpret.
 */
final readonly class ResultCaster
{
    public function cast(string $value, ?string $typeName): bool|float|int|string
    {
        return match ($typeName) {
            'bool' => $value === 't',
            'int2', 'int4' => (int) $value,
            'int8' => PHP_INT_SIZE >= 8 ? (int) $value : $value,
            'float4', 'float8' => $this->castFloat($value),
            'bytea' => $this->castBytea($value),
            'timestamp' => $this->castTimestamp($value),
            default => $value,
        };
    }

    private function castBytea(string $value): string
    {
        $decoded = hex2bin(substr($value, 2));

        if ($decoded === false) {
            throw ValueConversionException::invalidByteaData($value);
        }

        return $decoded;
    }

    /**
     * `timestamp` (without time zone) values are stored in UTC by TimestampConverter.
     * The database returns them without an offset, so we tag them as UTC to keep the
     * instant intact once a higher layer parses the string into a DateTimeInterface.
     */
    private function castTimestamp(string $value): string
    {
        if ($value === 'infinity' || $value === '-infinity') {
            return $value;
        }

        return $value . '+00:00';
    }

    private function castFloat(string $value): float
    {
        return match ($value) {
            'Infinity' => INF,
            '-Infinity' => -INF,
            'NaN' => NAN,
            default => floatval($value),
        };
    }
}
