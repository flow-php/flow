<?php

declare(strict_types=1);

namespace Flow\ETL\Function\Between;

use Flow\ETL\Function\ValueOrder;

enum Boundary
{
    case EXCLUSIVE;
    case INCLUSIVE;
    case LEFT_INCLUSIVE;
    case RIGHT_INCLUSIVE;

    public function compare(
        mixed $value,
        mixed $lowerBound,
        mixed $upperbound,
        ValueOrder $order = new ValueOrder(),
    ): ?bool {
        if ($value === null) {
            return null;
        }

        $aboveLower = null;

        if ($lowerBound !== null) {
            $aboveLower = match ($this) {
                self::INCLUSIVE, self::LEFT_INCLUSIVE => $order->compare($lowerBound, $value) <= 0,
                self::EXCLUSIVE, self::RIGHT_INCLUSIVE => $order->compare($lowerBound, $value) < 0,
            };
        }

        $belowUpper = null;

        if ($upperbound !== null) {
            $belowUpper = match ($this) {
                self::INCLUSIVE, self::RIGHT_INCLUSIVE => $order->compare($value, $upperbound) <= 0,
                self::EXCLUSIVE, self::LEFT_INCLUSIVE => $order->compare($value, $upperbound) < 0,
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
}
