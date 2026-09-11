<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Between;

use DateInterval;
use DateTimeImmutable;
use DateTimeInterface;
use Flow\ETL\Exception\InvalidArgumentException;

use function is_array;
use function is_bool;
use function is_numeric;
use function is_string;

enum Boundary
{
    case EXCLUSIVE;
    case INCLUSIVE;
    case LEFT_INCLUSIVE;
    case RIGHT_INCLUSIVE;

    public function compare(mixed $value, mixed $lowerBound, mixed $upperbound): ?bool
    {
        if ($value === null) {
            return null;
        }

        $aboveLower = null;

        if ($lowerBound !== null) {
            $aboveLower = match ($this) {
                self::INCLUSIVE, self::LEFT_INCLUSIVE => $this->compareValues($value, $lowerBound) >= 0,
                self::EXCLUSIVE, self::RIGHT_INCLUSIVE => $this->compareValues($value, $lowerBound) > 0,
            };
        }

        $belowUpper = null;

        if ($upperbound !== null) {
            $belowUpper = match ($this) {
                self::INCLUSIVE, self::RIGHT_INCLUSIVE => $this->compareValues($value, $upperbound) <= 0,
                self::EXCLUSIVE, self::LEFT_INCLUSIVE => $this->compareValues($value, $upperbound) < 0,
            };
        }

        // SQL AND: a definite FALSE short-circuits past a NULL bound (5 BETWEEN 10 AND NULL is FALSE).
        if ($aboveLower === false || $belowUpper === false) {
            return false;
        }

        if ($aboveLower === null || $belowUpper === null) {
            return null;
        }

        return true;
    }

    public function compareValues(mixed $left, mixed $right): int
    {
        if (is_numeric($left) && is_numeric($right)) {
            return (float) $left <=> (float) $right;
        }

        if (is_string($left) && is_string($right)) {
            return $left <=> $right;
        }

        if ($left instanceof DateTimeInterface && $right instanceof DateTimeInterface) {
            return $left <=> $right;
        }

        if ($left instanceof DateInterval && $right instanceof DateInterval) {
            $reference = new DateTimeImmutable('@0');

            return $reference->add($left) <=> $reference->add($right);
        }

        if (is_bool($left) && is_bool($right)) {
            return $left <=> $right;
        }

        if (is_array($left) && is_array($right)) {
            return $left <=> $right;
        }

        throw new InvalidArgumentException(
            'Boundary::compare requires value and bounds to be of a comparable type (numeric, string, DateTimeInterface, DateInterval, bool, or array).',
        );
    }
}
