<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile;

enum Compressions : int
{
    case BROTLI = 4;
    case GZIP = 2;
    case LZ4 = 5;
    case LZ4_RAW = 7;
    case LZO = 3;
    case SNAPPY = 1;
    case UNCOMPRESSED = 0;
    case ZSTD = 6;
}
