<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_boolean;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<bool>
 */
final class BooleanConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_boolean();
    }

    public function supportedTypes() : array
    {
        return [PostgreSqlType::BOOL];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        return $value ? 't' : 'f';
    }

    public function toPhp(string $value, PostgreSqlType $type) : bool
    {
        return $value === 't' || $value === 'true' || $value === '1';
    }
}
