<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\StringEscaper;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;
use JsonException;

use function array_map;
use function implode;
use function is_array;
use function json_encode;

final class JsonArrayConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [
            ValueType::JSON_ARRAY,
            ValueType::JSONB_ARRAY,
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

        try {
            return StringEscaper::escapeAlwaysQuoted(json_encode($element, JSON_THROW_ON_ERROR));
        } catch (JsonException) {
            throw ValueConversionException::cannotConvert($element, 'JSON array element');
        }
    }
}
