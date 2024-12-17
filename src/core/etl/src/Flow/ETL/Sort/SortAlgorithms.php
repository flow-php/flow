<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

enum SortAlgorithms
{
    case EXTERNAL_SORT;
    case MEMORY_FALLBACK_EXTERNAL_SORT;
    case MEMORY_SORT;
    case SQLITE_SORT;

    public function useMemory() : bool
    {
        return \in_array($this, [self::MEMORY_SORT, self::MEMORY_FALLBACK_EXTERNAL_SORT], true);
    }
}
