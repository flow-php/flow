<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Flow\Parquet\ThriftModel\MicroSeconds;
use Flow\Parquet\ThriftModel\MilliSeconds;
use Flow\Parquet\ThriftModel\NanoSeconds;
use Flow\Parquet\ThriftModel\TimeUnit as ThriftTimeUnit;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class TimeUnitTest extends TestCase
{
    #[TestWith(['MILLIS', TimeUnit::MILLISECONDS])]
    #[TestWith(['MICROS', TimeUnit::MICROSECONDS])]
    #[TestWith(['NANOS', TimeUnit::NANOSECONDS])]
    public function test_from_thrift_maps_each_unit(string $field, TimeUnit $expected): void
    {
        $unit = match ($field) {
            'MILLIS' => new MilliSeconds(),
            'MICROS' => new MicroSeconds(),
            default => new NanoSeconds(),
        };

        static::assertSame($expected, TimeUnit::fromThrift(new ThriftTimeUnit([$field => $unit])));
    }

    public function test_from_thrift_without_unit_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Unsupported time unit, expected one of MILLIS, MICROS, NANOS');

        TimeUnit::fromThrift(new ThriftTimeUnit());
    }
}
