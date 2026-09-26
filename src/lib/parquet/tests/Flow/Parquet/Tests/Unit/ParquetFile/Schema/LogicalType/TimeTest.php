<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema\LogicalType;

use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\ThriftModel\NanoSeconds;
use Flow\Parquet\ThriftModel\TimeType;
use Flow\Parquet\ThriftModel\TimeUnit as ThriftTimeUnit;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class TimeTest extends TestCase
{
    public function test_from_thrift(): void
    {
        $logicalType = Time::fromThrift(new TimeType([
            'isAdjustedToUTC' => true,
            'unit' => new ThriftTimeUnit(['NANOS' => new NanoSeconds()]),
        ]));

        static::assertTrue($logicalType->isAdjustedToUTC());
        static::assertSame(TimeUnit::NANOSECONDS, $logicalType->unit());
    }

    #[TestWith([TimeUnit::MILLISECONDS, true, false, false])]
    #[TestWith([TimeUnit::MICROSECONDS, false, true, false])]
    #[TestWith([TimeUnit::NANOSECONDS, false, false, true])]
    public function test_unit_flags_follow_the_unit(TimeUnit $unit, bool $millis, bool $micros, bool $nanos): void
    {
        $logicalType = new Time(false, $unit);

        static::assertSame($unit, $logicalType->unit());
        static::assertSame($millis, $logicalType->millis());
        static::assertSame($micros, $logicalType->micros());
        static::assertSame($nanos, $logicalType->nanos());
    }
}
