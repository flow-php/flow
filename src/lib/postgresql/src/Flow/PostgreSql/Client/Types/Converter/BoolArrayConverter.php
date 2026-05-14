<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

final class BoolArrayConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::BOOL_ARRAY];
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

        if (\is_bool($element)) {
            return $element ? 't' : 'f';
        }

        throw ValueConversionException::cannotConvert($element, 'boolean array element');
    }
}
