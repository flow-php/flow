<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_date;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<\DateTimeInterface>
 */
final class DateConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_date();
    }

    public function supportedTypes() : array
    {
        return [PostgreSqlType::DATE];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateTimeInterface) {
            return $value->format('Y-m-d');
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
