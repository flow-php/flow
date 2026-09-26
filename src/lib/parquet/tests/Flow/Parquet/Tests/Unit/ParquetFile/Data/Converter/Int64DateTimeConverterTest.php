<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\Int64DateTimeConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Timestamp;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class Int64DateTimeConverterTest extends TestCase
{
    public function test_converting_date_times(): void
    {
        $date = (new DateTimeImmutable('2021-01-01'))
            ->setTimezone(new DateTimeZone('UTC'))
            ->setTime(14, 56, 24, 54324);

        $converter = new Int64DateTimeConverter(TimeUnit::MICROSECONDS);

        static::assertEquals($date, $converter->fromParquetType($converter->toParquetType($date)));
    }

    public function test_for_column_reads_the_unit(): void
    {
        $converter = Int64DateTimeConverter::forColumn(
            new FlatColumn(
                'ts',
                PhysicalType::INT64,
                logicalType: new LogicalType(
                    LogicalType::TIMESTAMP,
                    timestamp: new Timestamp(false, TimeUnit::MILLISECONDS),
                ),
            ),
            Options::default(),
        );

        static::assertNotNull($converter);
        static::assertSame(
            '2020-01-02 03:04:05.678000',
            $converter->fromParquetType(1577934245678)->format('Y-m-d H:i:s.u'),
        );
    }

    public function test_for_column_rejects_int32_and_untyped_int64(): void
    {
        static::assertNull(Int64DateTimeConverter::forColumn(
            new FlatColumn('ts', PhysicalType::INT32, logicalType: LogicalType::timestamp()),
            Options::default(),
        ));
        static::assertNull(Int64DateTimeConverter::forColumn(FlatColumn::int64('id'), Options::default()));
    }

    #[TestWith([TimeUnit::MILLISECONDS, 1577934245678, '2020-01-02 03:04:05.678000 +00:00'])]
    #[TestWith([TimeUnit::MICROSECONDS, 1577934245678901, '2020-01-02 03:04:05.678901 +00:00'])]
    #[TestWith([TimeUnit::NANOSECONDS, 1577934245678901234, '2020-01-02 03:04:05.678901 +00:00'])]
    #[TestWith([TimeUnit::MILLISECONDS, -1500, '1969-12-31 23:59:58.500000 +00:00'])]
    #[TestWith([TimeUnit::MICROSECONDS, -1, '1969-12-31 23:59:59.999999 +00:00'])]
    #[TestWith([TimeUnit::NANOSECONDS, -1500, '1969-12-31 23:59:59.999998 +00:00'])]
    #[TestWith([TimeUnit::MICROSECONDS, 9223372036854775, '2262-04-11 23:47:16.854775 +00:00'])]
    #[TestWith([TimeUnit::MICROSECONDS, -9223372036854775, '1677-09-21 00:12:43.145225 +00:00'])]
    public function test_from_parquet_type(TimeUnit $unit, int $value, string $expected): void
    {
        static::assertSame(
            $expected,
            (new Int64DateTimeConverter($unit))
                ->fromParquetType($value)
                ->format('Y-m-d H:i:s.u P'),
        );
    }

    #[TestWith([TimeUnit::MILLISECONDS, '2020-01-02 04:04:05.678901 +01:00', 1577934245678])]
    #[TestWith([TimeUnit::MICROSECONDS, '2020-01-02 04:04:05.678901 +01:00', 1577934245678901])]
    #[TestWith([TimeUnit::NANOSECONDS, '2020-01-02 04:04:05.678901 +01:00', 1577934245678901000])]
    #[TestWith([TimeUnit::MILLISECONDS, '1969-12-31 23:59:58.5 UTC', -1500])]
    public function test_to_parquet_type(TimeUnit $unit, string $dateTime, int $expected): void
    {
        static::assertSame(
            $expected,
            (new Int64DateTimeConverter($unit))->toParquetType(new DateTimeImmutable($dateTime)),
        );
    }

    public function test_to_parquet_type_outside_nanos_range_throws(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage(
            'DateTime 2262-04-12T00:00:00+00:00 is outside the TIMESTAMP(NANOS) range 1677-09-21 – 2262-04-11',
        );

        (new Int64DateTimeConverter(TimeUnit::NANOSECONDS))->toParquetType(
            new DateTimeImmutable('2262-04-12 00:00:00 UTC'),
        );
    }
}
