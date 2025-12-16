<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_time;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<\DateInterval>
 */
final class IntervalConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_time();
    }

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

        return '';
    }

    public function toPhp(string $value, PostgreSqlType $type) : \DateInterval
    {
        return $this->parseInterval($value);
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

    private function parseInterval(string $value) : \DateInterval
    {
        if (\str_starts_with($value, 'P')) {
            return new \DateInterval($value);
        }

        $interval = new \DateInterval('PT0S');

        if (\preg_match('/(\d+)\s*years?/', $value, $matches)) {
            $interval->y = (int) $matches[1];
        }

        if (\preg_match('/(\d+)\s*mons?(?:ths?)?/', $value, $matches)) {
            $interval->m = (int) $matches[1];
        }

        if (\preg_match('/(\d+)\s*days?/', $value, $matches)) {
            $interval->d = (int) $matches[1];
        }

        if (\preg_match('/(\d{1,2}):(\d{2}):(\d{2})(?:\.(\d+))?/', $value, $matches)) {
            $interval->h = (int) $matches[1];
            $interval->i = (int) $matches[2];
            $interval->s = (int) $matches[3];

            if (isset($matches[4])) {
                $interval->f = (float) ('0.' . $matches[4]);
            }
        }

        return $interval;
    }
}
