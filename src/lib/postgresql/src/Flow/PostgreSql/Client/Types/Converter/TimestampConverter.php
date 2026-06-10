<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use DateTimeImmutable;
use DateTimeInterface;
use DateTimeZone;
use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\ValueConverter;
use Flow\PostgreSql\Client\Types\ValueType;

use function is_string;

/**
 * Converts values to PostgreSQL `timestamp` (without time zone).
 *
 * PostgreSQL discards any offset on a `timestamp` column, so a DateTimeInterface
 * is first normalized to UTC and then formatted without an offset. This keeps the
 * stored wall-clock in UTC and preserves the instant instead of letting the
 * database silently drop the timezone. For `timestamptz` use {@see TimestampTzConverter}.
 */
final class TimestampConverter implements ValueConverter
{
    public function supportedTypes(): array
    {
        return [ValueType::TIMESTAMP];
    }

    public function toDatabase(mixed $value): ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof DateTimeInterface) {
            return DateTimeImmutable::createFromInterface($value)
                ->setTimezone(new DateTimeZone('UTC'))
                ->format('Y-m-d H:i:s.u');
        }

        if (is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'timestamp');
    }
}
