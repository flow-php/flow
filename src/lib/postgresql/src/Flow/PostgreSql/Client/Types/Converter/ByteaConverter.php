<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final class ByteaConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::BYTEA];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return '\x' . \bin2hex($value);
        }

        throw ValueConversionException::cannotConvert($value, 'bytea');
    }
}
