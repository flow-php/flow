<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class DateConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [ValueType::DATE];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d');
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'date');
    }
}
