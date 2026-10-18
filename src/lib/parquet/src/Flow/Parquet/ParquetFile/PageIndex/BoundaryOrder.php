<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\PageIndex;

enum BoundaryOrder: int
{
    case ASCENDING = 1;
    case DESCENDING = 2;
    case UNORDERED = 0;
}
