<?php declare(strict_types=1);

namespace Flow\Parquet;

enum ByteOrder : string
{
    case BIG_ENDIAN = 'N';
    case LITTLE_ENDIAN = 'V';
}
