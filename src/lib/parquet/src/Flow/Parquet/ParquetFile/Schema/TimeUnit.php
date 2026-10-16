<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ThriftModel\TimeUnit as ThriftTimeUnit;

enum TimeUnit: string
{
    case MILLISECONDS = 'MILLIS';
    case MICROSECONDS = 'MICROS';
    case NANOSECONDS = 'NANOS';

    public static function fromThrift(ThriftTimeUnit $unit): self
    {
        if ($unit->MILLIS !== null) {
            return self::MILLISECONDS;
        }

        if ($unit->MICROS !== null) {
            return self::MICROSECONDS;
        }

        if ($unit->NANOS !== null) {
            return self::NANOSECONDS;
        }

        throw new InvalidArgumentException('Unsupported time unit, expected one of MILLIS, MICROS, NANOS');
    }
}
