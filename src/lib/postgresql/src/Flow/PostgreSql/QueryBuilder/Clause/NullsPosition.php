<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\SortByNulls;

/**
 * Nulls position enum for ORDER BY.
 */
enum NullsPosition : string
{
    case DEFAULT = 'DEFAULT';
    case FIRST = 'FIRST';
    case LAST = 'LAST';

    public static function fromProtobuf(int $sortByNulls) : self
    {
        return match ($sortByNulls) {
            SortByNulls::SORTBY_NULLS_FIRST => self::FIRST,
            SortByNulls::SORTBY_NULLS_LAST => self::LAST,
            default => self::DEFAULT,
        };
    }

    public function toProtobuf() : int
    {
        return match ($this) {
            self::FIRST => SortByNulls::SORTBY_NULLS_FIRST,
            self::LAST => SortByNulls::SORTBY_NULLS_LAST,
            self::DEFAULT => SortByNulls::SORTBY_NULLS_DEFAULT,
        };
    }
}
