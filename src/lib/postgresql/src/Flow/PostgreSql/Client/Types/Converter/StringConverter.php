<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class StringConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::TEXT,
            PostgreSqlType::VARCHAR,
            PostgreSqlType::CHAR,
            PostgreSqlType::BPCHAR,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        if ($value instanceof \Stringable) {
            return (string) $value;
        }

        throw ValueConversionException::cannotConvert($value, 'string');
    }
}
