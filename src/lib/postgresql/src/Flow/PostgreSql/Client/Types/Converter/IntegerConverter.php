<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class IntegerConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::INT2,
            PostgreSqlType::INT4,
            PostgreSqlType::INT8,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_int($value)) {
            return (string) $value;
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'integer');
    }
}
