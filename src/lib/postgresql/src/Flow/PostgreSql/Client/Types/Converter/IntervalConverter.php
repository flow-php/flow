<?php

declare(strict_types=1);

namespace Flow\PostgreSql\Client\Types\Converter;

use function Flow\Types\DSL\type_string;
use Flow\PostgreSql\Client\Types\{PostgreSqlType, ValueConverter};

use Flow\Types\Type;

/**
 * @implements ValueConverter<string>
 */
final class IntervalConverter implements ValueConverter
{
    public function flowType() : Type
    {
        return type_string();
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

    public function toPhp(string $value, PostgreSqlType $type) : string
    {
        return $value;
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
