<?php

declare(strict_types=1);

namespace Flow\PostgreSql\QueryBuilder\Clause;

use Flow\PostgreSql\Protobuf\AST\SortByDir;

/**
 * Sort direction enum.
 */
enum SortDirection: string
{
    case ASC = 'ASC';
    case DEFAULT = 'DEFAULT';
    case DESC = 'DESC';

    public static function fromProtobuf(int $sortByDir): self
    {
        return match ($sortByDir) {
            SortByDir::SORTBY_ASC => self::ASC,
            SortByDir::SORTBY_DESC => self::DESC,
            default => self::DEFAULT,
        };
    }

    public function toProtobuf(): int
    {
        return match ($this) {
            self::ASC => SortByDir::SORTBY_ASC,
            self::DESC => SortByDir::SORTBY_DESC,
            self::DEFAULT => SortByDir::SORTBY_DEFAULT,
        };
    }
}
