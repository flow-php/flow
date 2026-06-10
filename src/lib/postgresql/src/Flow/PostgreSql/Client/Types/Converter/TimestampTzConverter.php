<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use DateTimeInterface;
use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

use function is_string;

/**
 * Converts values to PostgreSQL `timestamptz` (timestamp with time zone).
 *
 * The timezone offset is kept in the formatted value; PostgreSQL reads it and
 * normalizes the instant to UTC on store. For naive `timestamp` columns use
 * {@see TimestampConverter} instead.
 */
final class TimestampTzConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::TIMESTAMPTZ];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof DateTimeInterface) {
            return $value->format('Y-m-d H:i:s.uP');
        }

        if (is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'timestamptz');
    }
}
