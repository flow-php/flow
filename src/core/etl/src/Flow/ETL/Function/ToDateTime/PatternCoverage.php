<?php

declare(strict_types=1);

namespace Flow\ETL\Function\ToDateTime;

use function str_contains;
use function strlen;

/**
 * Which fields a DateTimeImmutable::createFromFormat() pattern parses. Without `!` or `|` every field the pattern
 * leaves out comes from the clock: no time field means the current time of day, a missing date part means today's.
 * One parsed time field resets the other time fields.
 */
final readonly class PatternCoverage
{
    public function __construct(
        private string $pattern,
    ) {}

    public function fillsFromClock(): bool
    {
        $parsed = '';

        for ($i = 0, $length = strlen($this->pattern); $i < $length; $i++) {
            if ($this->pattern[$i] === '\\') {
                $i++;

                continue;
            }

            $parsed .= $this->pattern[$i];
        }

        if ($this->any($parsed, '!|U')) {
            return false;
        }

        $date =
            $this->any($parsed, 'YyXxo')
            && ($this->any($parsed, 'z') || $this->any($parsed, 'mnMF') && $this->any($parsed, 'dj'));

        return !$date || !$this->any($parsed, 'HGhgisvu');
    }

    public function any(string $parsed, string $characters): bool
    {
        for ($i = 0, $length = strlen($characters); $i < $length; $i++) {
            if (str_contains($parsed, $characters[$i])) {
                return true;
            }
        }

        return false;
    }
}
