<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\StringEscaper;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

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

        if (!\is_array($value)) {
            return '{}';
        }

        $elements = [];

        foreach ($value as $v) {
            if ($v === null) {
                $elements[] = 'NULL';
            } else {
                try {
                    $json = \json_encode($v, JSON_THROW_ON_ERROR);
                    $elements[] = StringEscaper::escapeAlwaysQuoted($json);
                } catch (\JsonException) {
                    throw ValueConversionException::cannotConvert($v, 'JSON array element');
                }
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
