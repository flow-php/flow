<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

use function is_float;
use function is_int;
use function is_string;

final class NumericConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::NUMERIC];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (is_string($value)) {
            return $value;
        }

        if (is_int($value) || is_float($value)) {
            return (string) $value;
        }

        throw ValueConversionException::cannotConvert($value, 'numeric');
    }
}
