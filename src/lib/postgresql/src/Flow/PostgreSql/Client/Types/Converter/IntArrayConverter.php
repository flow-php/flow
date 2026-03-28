<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{ValueConverter, ValueType};

final class IntArrayConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [
            ValueType::INT2_ARRAY,
            ValueType::INT4_ARRAY,
            ValueType::INT8_ARRAY,
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
            } elseif (\is_int($v)) {
                $elements[] = (string) $v;
            } elseif (\is_string($v) && \is_numeric($v)) {
                $elements[] = (string) (int) $v;
            } elseif (\is_float($v)) {
                $elements[] = (string) (int) $v;
            } else {
                throw ValueConversionException::cannotConvert($v, 'integer array element');
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
