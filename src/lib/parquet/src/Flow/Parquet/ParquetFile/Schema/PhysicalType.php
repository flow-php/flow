<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

enum PhysicalType : int
{
    case BOOLEAN = 0;
    case BYTE_ARRAY = 6;
    case DOUBLE = 5;
    case FIXED_LEN_BYTE_ARRAY = 7;
    case FLOAT = 4;
    case INT32 = 1;
    case INT64 = 2;
    case INT96 = 3;  // deprecated, only used by legacy implementations.
}
