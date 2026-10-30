<?php

declare(strict_types=1);

namespace Flow\ETL\Values;

enum NullsOrder
{
    case FIRST;
    case LAST;

    /**
     * Nulls are the smallest value: first ascending, last descending.
     */
    public static function defaultFor(SortOrder $order): self
    {
        return $order === SortOrder::ASC ? self::FIRST : self::LAST;
    }
}
