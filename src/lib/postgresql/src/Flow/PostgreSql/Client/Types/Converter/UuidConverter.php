<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_uuid;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};
use Flow\Types\Type;

use Flow\Types\Value\Uuid;

/**
 * @implements ValueConverter<Uuid>
 */
final class UuidConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_uuid();
    }

    public function supportedTypes() : array
    {
        return [PostgreSqlType::UUID];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof Uuid) {
            return $value->toString();
        }

        if (\is_string($value)) {
            return $value;
        }

        return '';
    }

    public function toPhp(string $value, PostgreSqlType $type) : Uuid
    {
        return Uuid::fromString($value);
    }
}
