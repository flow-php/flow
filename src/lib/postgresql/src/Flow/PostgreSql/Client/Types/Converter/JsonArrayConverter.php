<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class JsonArrayConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::JSON_ARRAY,
            PostgreSqlType::JSONB_ARRAY,
        ];
    }

    public function toDatabase(mixed $value) : ?string
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
                    $elements[] = \json_encode($v, JSON_THROW_ON_ERROR);
                } catch (\JsonException) {
                    throw ValueConversionException::cannotConvert($v, 'JSON array element');
                }
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
