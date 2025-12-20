<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class UuidArrayConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [PostgreSqlType::UUID_ARRAY];
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
                $elements[] = $v;
            } else {
                throw ValueConversionException::cannotConvert($v, 'UUID array element');
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
