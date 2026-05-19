<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Between;

use DateInterval;
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

    public function compare(mixed $value, mixed $lowerBound, mixed $upperbound): bool
    {
        if (is_numeric($value) && is_numeric($lowerBound) && is_numeric($upperbound)) {
            $v = (float) $value;
            $l = (float) $lowerBound;
            $u = (float) $upperbound;

            return match ($this) {
                self::INCLUSIVE => $v >= $l && $v <= $u,
                self::EXCLUSIVE => $v > $l && $v < $u,
                self::LEFT_INCLUSIVE => $v >= $l && $v < $u,
                self::RIGHT_INCLUSIVE => $v > $l && $v <= $u,
            };
        }

        if (is_string($value) && is_string($lowerBound) && is_string($upperbound)) {
            return match ($this) {
                self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
                self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
                self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
                self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
            };
        }

        if (
            $value instanceof DateTimeInterface
            && $lowerBound instanceof DateTimeInterface
            && $upperbound instanceof DateTimeInterface
        ) {
            return match ($this) {
                self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
                self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
                self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
                self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
            };
        }

        if (
            $value instanceof DateInterval
            && $lowerBound instanceof DateInterval
            && $upperbound instanceof DateInterval
        ) {
            return match ($this) {
                self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
                self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
                self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
                self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
            };
        }

        if (is_bool($value) && is_bool($lowerBound) && is_bool($upperbound)) {
            return match ($this) {
                self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
                self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
                self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
                self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
            };
        }

        if (is_array($value) && is_array($lowerBound) && is_array($upperbound)) {
            return match ($this) {
                self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
                self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
                self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
                self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
            };
        }

        throw new InvalidArgumentException(
            'Boundary::compare requires value, lowerBound and upperBound to be of a comparable type (numeric, string, DateTimeInterface, DateInterval, bool, or array).',
        );
    }
}
