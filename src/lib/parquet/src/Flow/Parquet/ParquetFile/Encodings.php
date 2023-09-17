<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

enum Encodings : int
{
    case BIT_PACKED = 4;
    case BYTE_STREAM_SPLIT = 9;
    case DELTA_BINARY_PACKED = 5;
    case DELTA_BYTE_ARRAY = 7;
    case DELTA_LENGTH_BYTE_ARRAY = 6;
    case PLAIN = 0;
    case PLAIN_DICTIONARY = 2;
    case RLE = 3;
    case RLE_DICTIONARY = 8;
}
