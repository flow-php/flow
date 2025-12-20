<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use Flow\PostgreSql\Client\Exception\ValueConversionException;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

final class IntervalConverter implements ValueConverter
{
    public function supportedTypes() : array
    {
        return [PostgreSqlType::INTERVAL];
    }

    public function toDatabase(mixed $value) : ?string
    {
        if ($value === null) {
            return null;
        }

        if ($value instanceof \DateInterval) {
            return $this->formatInterval($value);
        }

        if (\is_string($value)) {
            return $value;
        }

        throw ValueConversionException::cannotConvert($value, 'interval');
    }

    private function formatInterval(\DateInterval $interval) : string
    {
        $parts = [];

        if ($interval->y) {
            $parts[] = $interval->y . ' year' . ($interval->y > 1 ? 's' : '');
        }

        if ($interval->m) {
            $parts[] = $interval->m . ' month' . ($interval->m > 1 ? 's' : '');
        }

        if ($interval->d) {
            $parts[] = $interval->d . ' day' . ($interval->d > 1 ? 's' : '');
        }

        if ($interval->h || $interval->i || $interval->s) {
            $parts[] = \sprintf('%02d:%02d:%02d', $interval->h, $interval->i, $interval->s);
        }

        return \implode(' ', $parts) ?: '0';
    }
}
