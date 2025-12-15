<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_datetime;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<\DateTimeInterface>
 */
final class TimeConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_datetime();
    }

    public function supportedTypes() : array
    {
        return [
            PostgreSqlType::TIME,
            PostgreSqlType::TIMETZ,
        ];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('H:i:s.u');
        }

        if (\is_string($value)) {
            return $value;
        }

        return '';
    }

    public function toPhp(string $value, PostgreSqlType $type) : \DateTimeInterface
    {
        return new \DateTimeImmutable($value);
    }
}
