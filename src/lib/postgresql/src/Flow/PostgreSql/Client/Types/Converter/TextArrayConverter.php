<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\StringEscaper;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final class TextArrayConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::TEXT_ARRAY,
            ValueType::VARCHAR_ARRAY,
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

        if (\is_string($element)) {
            return StringEscaper::escape($element);
        }

        if (\is_scalar($element)) {
            return (string) $element;
        }

        throw ValueConversionException::cannotConvert($element, 'text array element');
    }
}
