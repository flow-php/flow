<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

enum ConvertedType : int
{
    case BSON = 20;
    case DATE = 6;
    case DECIMAL = 5;
    case ENUM = 4;
    case INT_16 = 16;
    case INT_32 = 17;
    case INT_64 = 18;
    case INT_8 = 15;
    case INTERVAL = 21;
    case JSON = 19;
    case LIST = 3;
    case MAP = 1;
    case MAP_KEY_VALUE = 2;
    case TIME_MICROS = 8;
    case TIME_MILLIS = 7;
    case TIMESTAMP_MICROS = 10;
    case TIMESTAMP_MILLIS = 9;
    case UINT_16 = 12;
    case UINT_32 = 13;
    case UINT_64 = 14;
    case UINT_8 = 11;
    case UTF8 = 0;
}
