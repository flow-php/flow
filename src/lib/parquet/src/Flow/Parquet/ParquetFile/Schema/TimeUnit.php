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
        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($unit->MILLIS !== null) {
            return self::MILLISECONDS;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($unit->MICROS !== null) {
            return self::MICROSECONDS;
        }

        // @mago-ignore analysis:redundant-condition
        // @mago-ignore analysis:redundant-comparison
        if ($unit->NANOS !== null) {
            return self::NANOSECONDS;
        }

        throw new InvalidArgumentException('Unsupported time unit, expected one of MILLIS, MICROS, NANOS');
    }
}
