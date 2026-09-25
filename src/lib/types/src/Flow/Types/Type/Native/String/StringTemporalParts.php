<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native\String;

use Flow\Types\Type\Logical\DateTimeType;
use Flow\Types\Type\Logical\DateType;

use function checkdate;
use function date_parse;
use function Flow\Types\DSL\type_integer;
use function is_array;
use function preg_match;
use function preg_match_all;

final readonly class StringTemporalParts
{
    public function __construct(
        private bool $calendarDate,
        private bool $time,
    ) {}

    public static function from(string $value): self
    {
        if (!self::hasExplicitDay($value)) {
            return new self(false, false);
        }

        if (self::isoDateTime($value)) {
            return new self(true, true);
        }

        if (self::isoDate($value)) {
            return new self(true, false);
        }

        $parts = date_parse($value);

        if (
            type_integer()->assert($parts['error_count']) > 0
            || $parts['year'] === false
            || $parts['month'] === false
            || $parts['day'] === false
            || !checkdate((int) $parts['month'], (int) $parts['day'], (int) $parts['year'])
        ) {
            return new self(false, false);
        }

        $time =
            ($parts['hour'] ?? false) !== false
            || ($parts['minute'] ?? false) !== false
            || ($parts['second'] ?? false) !== false
            || ($parts['fraction'] ?? false) !== false;

        if (!$time && is_array($parts['relative'] ?? false)) {
            /** @var array<string, mixed> $relative */
            $relative = $parts['relative'];
            $time =
                ($relative['hour'] ?? 0) !== 0 || ($relative['minute'] ?? 0) !== 0 || ($relative['second'] ?? 0) !== 0;
        }

        return new self(true, $time);
    }

    public static function isoDateTime(string $value): bool
    {
        $iso = [];

        return (
            preg_match(DateTimeType::ISO_DATE_TIME, $value, $iso) === 1
            && checkdate((int) $iso[2], (int) $iso[3], (int) $iso[1])
        );
    }

    public static function isoDate(string $value): bool
    {
        $iso = [];

        return (
            preg_match(DateType::ISO_DATE, $value, $iso) === 1
            && checkdate((int) $iso[2], (int) $iso[3], (int) $iso[1])
        );
    }

    public static function hasExplicitDay(string $value): bool
    {
        $numericGroups = (int) preg_match_all('/\d+/', $value);

        if ($numericGroups >= 3) {
            return true;
        }

        if ($numericGroups === 1 && preg_match('/^\d{8}$/', $value) === 1) {
            return true;
        }

        return $numericGroups >= 2 && preg_match('/[A-Za-z]{3,}/', $value) === 1;
    }

    public function isDate(): bool
    {
        return $this->calendarDate && !$this->time;
    }

    public function isDateTime(): bool
    {
        return $this->calendarDate && $this->time;
    }
}
