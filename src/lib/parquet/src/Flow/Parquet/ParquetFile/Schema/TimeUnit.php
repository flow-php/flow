<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

enum TimeUnit
{
    // case MILLISECONDS; Not Implemented yet
    case MICROSECONDS;
    // case NANOSECONDS; PHP Does not support nanoseconds
}
