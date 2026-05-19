<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

use function array_map;
use function implode;
use function is_array;
use function is_float;
use function is_int;
use function is_numeric;
use function is_string;

final class FloatArrayConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::FLOAT4_ARRAY,
            ValueType::FLOAT8_ARRAY,
        ];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if (!is_array($value)) {
            return '{}';
        }

        return '{' . implode(',', array_map(self::encodeElement(...), $value)) . '}';
    }

    private static function encodeElement(mixed $element): string
    {
        if ($element === null) {
            return 'NULL';
        }

        if (is_float($element)) {
            return (string) $element;
        }

        if (is_int($element)) {
            return (string) (float) $element;
        }

        if (is_string($element) && is_numeric($element)) {
            return (string) (float) $element;
        }

        throw ValueConversionException::cannotConvert($element, 'float array element');
    }
}
