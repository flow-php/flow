<?php

declare(strict_types=1);

namespace Flow\ETL\Sort;

enum SortAlgorithms
{
    case EXTERNAL_SORT;
    case MEMORY_SORT;
}
