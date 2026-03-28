<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{StringEscaper, ValueConverter, ValueType};

final class TextArrayConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            ValueType::TEXT_ARRAY,
            ValueType::VARCHAR_ARRAY,
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
            } elseif (\is_string($v)) {
                $elements[] = StringEscaper::escape($v);
            } elseif (\is_scalar($v)) {
                $elements[] = (string) $v;
            } else {
                throw ValueConversionException::cannotConvert($v, 'text array element');
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
