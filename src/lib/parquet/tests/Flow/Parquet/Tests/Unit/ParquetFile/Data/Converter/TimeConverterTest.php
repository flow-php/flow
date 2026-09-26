<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use DateTimeImmutable;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\TimeConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class TimeConverterTest extends TestCase
{
    public function test_converting_diff_between_two_datetimes_to_time(): void
    {
        $start = new DateTimeImmutable('2023-01-01 00:00:00 UTC');
        $end = (new DateTimeImmutable('2023-01-01 00:00:0 UTC'))->setTime(15, 25, 30, 15040);

        $diff = $start->diff($end);
        $converter = new TimeConverter(TimeUnit::MICROSECONDS);

        static::assertEquals($diff, $converter->fromParquetType($converter->toParquetType($diff)));
    }

    public function test_for_column_on_int32_time_millis(): void
    {
        $converter = TimeConverter::forColumn(
            new FlatColumn(
                't',
                PhysicalType::INT32,
                logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS)),
            ),
            Options::default(),
        );

        static::assertNotNull($converter);
        static::assertSame('03:04:05.678000', $converter->fromParquetType(11045678)->format('%H:%I:%S.%F'));
    }

    #[TestWith([TimeUnit::MILLISECONDS, 11045678, '03:04:05.678000'])]
    #[TestWith([TimeUnit::MICROSECONDS, 11045678901, '03:04:05.678901'])]
    #[TestWith([TimeUnit::NANOSECONDS, 11045678901234, '03:04:05.678901'])]
    #[TestWith([TimeUnit::NANOSECONDS, 999, '00:00:00.000000'])]
    public function test_from_parquet_type(TimeUnit $unit, int $value, string $expected): void
    {
        static::assertSame(
            $expected,
            (new TimeConverter($unit))
                ->fromParquetType($value)
                ->format('%H:%I:%S.%F'),
        );
    }

    #[TestWith([TimeUnit::MILLISECONDS, 11045678])]
    #[TestWith([TimeUnit::MICROSECONDS, 11045678901])]
    #[TestWith([TimeUnit::NANOSECONDS, 11045678901000])]
    public function test_to_parquet_type(TimeUnit $unit, int $expected): void
    {
        $interval = (new DateTimeImmutable('2020-01-01 00:00:00 UTC'))->diff(
            new DateTimeImmutable('2020-01-01 03:04:05.678901 UTC'),
        );

        static::assertSame($expected, (new TimeConverter($unit))->toParquetType($interval));
    }
}
