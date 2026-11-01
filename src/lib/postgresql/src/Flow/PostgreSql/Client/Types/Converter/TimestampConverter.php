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
 * Normalized to UTC first, since a timestamp column drops the offset.
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
