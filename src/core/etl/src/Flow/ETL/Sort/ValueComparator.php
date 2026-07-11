<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

use DateInterval;
use DateTimeInterface;

use function is_array;
use function is_numeric;
use function is_string;

final readonly class ValueComparator
{
    public static function compare(mixed $left, mixed $right): int
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
            return $left <=> $right;
        }

        if (is_array($left) && is_array($right)) {
            return $left <=> $right;
        }

        return 0;
    }
}
