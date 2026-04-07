<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class FloatConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            ValueType::FLOAT4,
            ValueType::FLOAT8,
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

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'float');
    }
}
