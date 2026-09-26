<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Schema;

use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use Generator;
use PHPUnit\Framework\Attributes\DataProvider;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class LogicalTypeTest extends TestCase
{
    /**
     * @return \Generator<string, array{ConvertedType, string, ?TimeUnit, ?bool, ?int, ?int}>
     */
    public static function converted_types_provider(): Generator
    {
        yield 'DATE' => [ConvertedType::DATE, LogicalType::DATE, null, null, null, null];
        yield 'DECIMAL' => [ConvertedType::DECIMAL, LogicalType::DECIMAL, null, null, 2, 9];
        yield 'TIME_MILLIS' => [
            ConvertedType::TIME_MILLIS,
            LogicalType::TIME,
            TimeUnit::MILLISECONDS,
            true,
            null,
            null,
        ];
        yield 'TIME_MICROS' => [
            ConvertedType::TIME_MICROS,
            LogicalType::TIME,
            TimeUnit::MICROSECONDS,
            true,
            null,
            null,
        ];
        yield 'TIMESTAMP_MILLIS' => [
            ConvertedType::TIMESTAMP_MILLIS,
            LogicalType::TIMESTAMP,
            TimeUnit::MILLISECONDS,
            true,
            null,
            null,
        ];
        yield 'TIMESTAMP_MICROS' => [
            ConvertedType::TIMESTAMP_MICROS,
            LogicalType::TIMESTAMP,
            TimeUnit::MICROSECONDS,
            true,
            null,
            null,
        ];
    }

    #[DataProvider('converted_types_provider')]
    public function test_from_converted_type(
        ConvertedType $convertedType,
        string $name,
        ?TimeUnit $unit,
        ?bool $isAdjustedToUTC,
        ?int $scale,
        ?int $precision,
    ): void {
        $logicalType = LogicalType::fromConvertedType($convertedType, 2, 9);

        static::assertNotNull($logicalType);
        static::assertSame($name, $logicalType->name());
        static::assertSame($unit, $logicalType->timestampData()?->unit() ?? $logicalType->timeData()?->unit());
        static::assertSame(
            $isAdjustedToUTC,
            $logicalType->timestampData()?->isAdjustedToUTC() ?? $logicalType->timeData()?->isAdjustedToUTC(),
        );
        static::assertSame($scale, $logicalType->decimalData()?->scale());
        static::assertSame($precision, $logicalType->decimalData()?->precision());
    }

    #[TestWith([ConvertedType::UTF8])]
    #[TestWith([ConvertedType::INT_32])]
    #[TestWith([ConvertedType::LIST])]
    public function test_from_converted_type_without_logical_counterpart(ConvertedType $convertedType): void
    {
        static::assertNull(LogicalType::fromConvertedType($convertedType, null, null));
    }

    public function test_time_is_local(): void
    {
        static::assertFalse(LogicalType::time()->timeData()?->isAdjustedToUTC());
        static::assertSame(TimeUnit::MICROSECONDS, LogicalType::time()->timeData()?->unit());
    }

    public function test_timestamp_is_an_utc_instant(): void
    {
        static::assertTrue(LogicalType::timestamp()->timestampData()?->isAdjustedToUTC());
        static::assertSame(TimeUnit::MICROSECONDS, LogicalType::timestamp()->timestampData()?->unit());
    }
}
