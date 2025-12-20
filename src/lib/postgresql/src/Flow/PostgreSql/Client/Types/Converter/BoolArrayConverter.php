<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class BoolArrayConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [PostgreSqlType::BOOL_ARRAY];
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
            } elseif (\is_bool($v)) {
                $elements[] = $v ? 't' : 'f';
            } else {
                throw ValueConversionException::cannotConvert($v, 'boolean array element');
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
