<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_integer;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<int>
 */
final class IntegerConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_integer();
    }

    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::INT2,
            PostgreSqlType::INT4,
            PostgreSqlType::INT8,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_int($value)) {
            return (string) $value;
        }

        if (\is_scalar($value)) {
            return (string) (int) $value;
        }

        return '0';
    }

    public function toPhp(string $value, PostgreSqlType $type) : int
    {
        return (int) $value;
    }
}
