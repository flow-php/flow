<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\Int32DateConverter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class Int32DateConverterTest extends TestCase
{
    public function test_converting_dates(): void
    {
        $date = new DateTimeImmutable('2021-01-01 00:00:00 UTC');

        $converter = new Int32DateConverter();

        static::assertEquals($date, $converter->fromParquetType($converter->toParquetType($date)));
    }

    public function test_converting_int32_with_deprecated_converted_type(): void
    {
        static::assertInstanceOf(Int32DateConverter::class, Int32DateConverter::forColumn(
            new FlatColumn('date', PhysicalType::INT32, ConvertedType::DATE, null),
            Options::default(),
        ));
    }

    #[TestWith(['2024-01-01 00:00:00', 'Europe/Warsaw', 19723])]
    #[TestWith(['2024-01-01 23:59:59', 'America/New_York', 19723])]
    #[TestWith(['1969-12-31 12:00:00', 'UTC', -1])]
    #[TestWith(['1969-12-31 00:00:00', 'Asia/Tokyo', -1])]
    #[TestWith(['1900-02-28 00:00:00', 'UTC', -25509])]
    public function test_to_parquet_type(string $dateTime, string $timeZone, int $expected): void
    {
        static::assertSame(
            $expected,
            (new Int32DateConverter())->toParquetType(new DateTimeImmutable($dateTime, new DateTimeZone($timeZone))),
        );
    }
}
