<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class BooleanConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [ValueType::BOOL];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_bool($value)) {
            return $value ? 't' : 'f';
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'boolean');
    }
}
