<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_float;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<float>
 */
final class FloatConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_float();
    }

    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::FLOAT4,
            PostgreSqlType::FLOAT8,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_float($value)) {
            if (\is_nan($value)) {
                return 'NaN';
            }

            if (\is_infinite($value)) {
                return $value > 0 ? 'Infinity' : '-Infinity';
            }

            return (string) $value;
        }

        if (\is_int($value)) {
            return (string) $value;
        }

        if (\is_scalar($value)) {
            return (string) (float) $value;
        }

        return '0';
    }

    public function toPhp(string $value, PostgreSqlType $type) : float
    {
        return match ($value) {
            'Infinity' => \INF,
            '-Infinity' => -\INF,
            'NaN' => \NAN,
            default => (float) $value,
        };
    }
}
