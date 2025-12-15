<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_string;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * Multirange type converter for PostgreSQL 14+.
 *
 * @implements ValueConverter<string>
 */
final class MultirangeConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_string();
    }

    public function supportedTypes() : array
    {
        return [];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if (\is_string($value)) {
            return $value;
        }

        return '';
    }

    public function toPhp(string $value, PostgreSqlType $type) : string
    {
        return $value;
    }
}
