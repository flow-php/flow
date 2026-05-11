<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final class JsonConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::JSON,
            ValueType::JSONB,
        ];
    }

    public function toDatabase(mixed $value): ?string
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

        if (\is_array($value)) {
            return \json_encode($value, \JSON_THROW_ON_ERROR);
        }

        throw ValueConversionException::cannotConvert($value, 'json');
    }
}
