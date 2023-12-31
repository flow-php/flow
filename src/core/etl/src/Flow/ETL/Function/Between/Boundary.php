<?php declare(strict_types=1);

namespace Flow\ETL\Function\Between;

enum Boundary
{
    case EXCLUSIVE;
    case INCLUSIVE;
    case LEFT_INCLUSIVE;
    case RIGHT_INCLUSIVE;

    public function compare(mixed $value, mixed $lowerBound, mixed $upperbound) : bool
    {
        return match ($this) {
            self::INCLUSIVE => $value >= $lowerBound && $value <= $upperbound,
            self::EXCLUSIVE => $value > $lowerBound && $value < $upperbound,
            self::LEFT_INCLUSIVE => $value >= $lowerBound && $value < $upperbound,
            self::RIGHT_INCLUSIVE => $value > $lowerBound && $value <= $upperbound,
        };
    }
}
