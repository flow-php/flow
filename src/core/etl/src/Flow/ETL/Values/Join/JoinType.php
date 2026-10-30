<?php

declare(strict_types=1);

namespace Flow\ETL\Values\Join;

enum JoinType: string
{
    case inner = 'inner';
    case left = 'left';
    case left_anti = 'left_anti';
    case right = 'right';

    /**
     * A row of $side whose key holds a null can match nothing, and is dropped unless the join keeps $side's unmatched
     * rows.
     */
    public function dropsNullKeys(Side $side): bool
    {
        return match ($side) {
            Side::left => $this === self::inner || $this === self::right,
            Side::right => $this !== self::right,
        };
    }

    /**
     * An unmatched row of $side is kept, merged with a null row of the other side.
     */
    public function padsWithNullRow(Side $side): bool
    {
        return match ($side) {
            Side::left => $this === self::left,
            Side::right => $this === self::right,
        };
    }
}
