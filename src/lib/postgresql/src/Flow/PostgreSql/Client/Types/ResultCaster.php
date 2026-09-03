<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types;

use Flow\PostgreSql\Client\Exception\ValueConversionException;

use function array_map;
use function array_values;
use function floatval;
use function hex2bin;
use function is_array;
use function preg_replace;
use function str_starts_with;
use function substr;

use const INF;
use const NAN;
use const PHP_INT_SIZE;

/**
 * Casts PostgreSQL result values to unambiguous PHP types.
 *
 * Uses string type names from pg_field_type() for portability (OIDs can vary with extensions).
 * Anything without an explicit arm stays a string for higher layers to interpret.
 */
final readonly class ResultCaster
{
    private ArrayLiteralParser $arrayLiteralParser;

    public function __construct()
    {
        $this->arrayLiteralParser = new ArrayLiteralParser();
    }

    /**
     * @return list<mixed>|bool|float|int|string
     */
    public function cast(string $value, ?string $typeName): array|bool|float|int|string
    {
        return match ($typeName) {
            'bool' => $value === 't',
            'int2', 'int4', 'oid' => (int) $value,
            'int8' => PHP_INT_SIZE >= 8 ? (int) $value : $value,
            'float4', 'float8' => $this->castFloat($value),
            'bytea' => $this->castBytea($value),
            'timestamp' => $this->castTimestamp($value),
            'timetz' => $this->castTimeTz($value),
            // Checked last so a typed column never pays for it. No array type name can reach an
            // explicit arm above: every one of them starts with an underscore.
            default => $typeName !== null && str_starts_with($typeName, '_')
                ? $this->castElements($this->arrayLiteralParser->parse($value), substr($typeName, 1))
                : $value,
        };
    }

    /**
     * pg carries an array's element type once, for every dimension, so a nested list re-enters the
     * same conversion rather than being re-parsed.
     *
     * @param array<array-key, mixed> $elements
     *
     * @return list<mixed>
     */
    private function castElements(array $elements, string $elementType): array
    {
        return array_map(fn(mixed $element): mixed => match (true) {
            $element === null => null,
            is_array($element) => $this->castElements($element, $elementType),
            default => $this->cast((string) $element, $elementType),
        }, array_values($elements));
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

    /**
     * TimeType is DateInterval-backed and carries no offset, so timetz keeps its clock time and
     * drops the zone. A stated, accepted loss.
     */
    private function castTimeTz(string $value): string
    {
        return preg_replace('/[+-]\d{2}(:\d{2}){0,2}$/', '', $value) ?? $value;
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
