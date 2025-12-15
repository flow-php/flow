<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_string;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<string>
 */
final class StringConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_string();
    }

    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::TEXT,
            PostgreSqlType::VARCHAR,
            PostgreSqlType::CHAR,
            PostgreSqlType::BPCHAR,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        if (\is_scalar($value) || (\is_object($value) && \method_exists($value, '__toString'))) {
            return (string) $value;
        }

        return '';
    }

    public function toPhp(string $value, PostgreSqlType $type) : string
    {
        return $value;
    }
}
