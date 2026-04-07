<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class UuidConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [ValueType::UUID];
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
            return $value->__toString();
        }

        throw ValueConversionException::cannotConvert($value, 'uuid');
    }
}
