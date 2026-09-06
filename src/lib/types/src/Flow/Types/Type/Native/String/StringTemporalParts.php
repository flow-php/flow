<?php

declare(strict_types=1);

namespace Flow\Types\Type\Native\String;

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
        private bool $explicitDay,
        private bool $time,
    ) {}

    /**
     * One date_parse() for both temporal rungs; running it per rung cost ~90% of narrow() on non-temporal cells.
     */
    public static function from(string $value): self
    {
        $parts = date_parse($value);

        if (
            type_integer()->assert($parts['error_count']) > 0
            || $parts['year'] === false
            || $parts['month'] === false
            || $parts['day'] === false
            || !checkdate((int) $parts['month'], (int) $parts['day'], (int) $parts['year'])
        ) {
            return new self(false, false, false);
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

        return new self(true, self::hasExplicitDay($value), $time);
    }

    /**
     * date_parse() defaults a missing day to 1, so '2024-01' is indistinguishable from '2024-01-01' by its parts
     * alone and a month-precision column would be typed date with a fabricated day. The day has to be read back
     * out of the input: three numeric groups, or two plus a spelled-out month ('02-Jun-2022').
     *
     * A month paired with a time ('2024-01 10:00') still reaches three groups; per-column format detection, which
     * is how DuckDB settles this, is recorded as debt rather than approximated further here.
     *
     * Compact ISO ('20240305') is one group and a real calendar date; from()'s checkdate() gate keeps '12345678'
     * and friends out, so the widening is exactly that one form.
     */
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
        return $this->calendarDate && $this->explicitDay && !$this->time;
    }

    public function isDateTime(): bool
    {
        return $this->calendarDate && $this->explicitDay && $this->time;
    }
}
