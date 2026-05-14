<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final class IntArrayConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::INT2_ARRAY,
            ValueType::INT4_ARRAY,
            ValueType::INT8_ARRAY,
        ];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (!\is_array($value)) {
            return '{}';
        }

        return '{' . \implode(',', \array_map(self::encodeElement(...), $value)) . '}';
    }

    private static function encodeElement(mixed $element): string
    {
        if ($element === null) {
            return 'NULL';
        }

        if (\is_int($element)) {
            return (string) $element;
        }

        if (\is_string($element) && \is_numeric($element)) {
            return (string) (int) $element;
        }

        if (\is_float($element)) {
            return (string) (int) $element;
        }

        throw ValueConversionException::cannotConvert($element, 'integer array element');
    }
}
