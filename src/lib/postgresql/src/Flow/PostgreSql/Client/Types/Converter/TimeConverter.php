<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class TimeConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            ValueType::TIME,
            ValueType::TIMETZ,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('H:i:s.u');
        }

        if ($value instanceof \DateInterval) {
            return \sprintf('%02d:%02d:%02d', $value->h, $value->i, $value->s);
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'time');
    }
}
