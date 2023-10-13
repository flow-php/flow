<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

enum Repetition : int
{
    case OPTIONAL = 1;
    case REPEATED = 2;
    case REQUIRED = 0;
}
