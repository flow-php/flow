<?php

declare(strict_types=1);

namespace Flow\ETL\Adapter\PostgreSql\Pagination;

use Flow\PostgreSql\AST\Transformers\SortOrder;

enum Order : string
{
    case ASC = 'ASC';
    case DESC = 'DESC';

    public function toSortOrder() : SortOrder
    {
        return match ($this) {
            self::ASC => SortOrder::ASC,
            self::DESC => SortOrder::DESC,
        };
    }
}
