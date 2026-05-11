<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

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

        if (!\is_array($value)) {
            return '{}';
        }

        $elements = [];

        foreach ($value as $v) {
            if ($v === null) {
                $elements[] = 'NULL';
            } elseif (\is_float($v)) {
                $elements[] = (string) $v;
            } elseif (\is_int($v)) {
                $elements[] = (string) (float) $v;
            } elseif (\is_string($v) && \is_numeric($v)) {
                $elements[] = (string) (float) $v;
            } else {
                throw ValueConversionException::cannotConvert($v, 'float array element');
            }
        }

        return '{' . \implode(',', $elements) . '}';
    }
}
