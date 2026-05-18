<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use Stringable;

use function is_bool;
use function is_float;
use function is_int;
use function is_string;

final class StringConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::TEXT,
            ValueType::VARCHAR,
            ValueType::CHAR,
            ValueType::BPCHAR,
        ];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value)) {
            return $value;
        }

        if ($value instanceof Stringable) {
            return (string) $value;
        }

        if (is_int($value) || is_float($value) || is_bool($value)) {
            return (string) $value;
        }

        throw ValueConversionException::cannotConvert($value, 'string');
    }
}
