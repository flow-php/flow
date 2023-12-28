<?php declare(strict_types=1);

namespace Flow\ETL\Function\Between;

enum Boundary : string
{
    case EXCLUSIVE = 'exclusive';
    case INCLUSIVE = 'inclusive';
    case LEFT_INCLUSIVE = 'leftInclusive';
    case RIGHT_INCLUSIVE = 'rightInclusive';

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
