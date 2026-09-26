<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use OverflowException;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class DecimalConverterTest extends TestCase
{
    public function test_for_column_ignores_fixed_len_byte_array_decimals(): void
    {
        static::assertNull(DecimalConverter::forColumn(FlatColumn::decimal('d', 9, 2), Options::default()));
    }

    #[TestWith([PhysicalType::INT32])]
    #[TestWith([PhysicalType::INT64])]
    public function test_for_column_on_int_backed_decimals(PhysicalType $type): void
    {
        $converter = DecimalConverter::forColumn(
            new FlatColumn('d', $type, logicalType: LogicalType::decimal(2, 9)),
            Options::default(),
        );

        static::assertNotNull($converter);
        static::assertSame(12345.67, $converter->fromParquetType(1234567));
    }

    #[TestWith([2, 1234567, 12345.67])]
    #[TestWith([2, -1, -0.01])]
    #[TestWith([0, 42, 42.0])]
    #[TestWith([2, 123456789012345678, 1234567890123456.8])]
    public function test_from_parquet_type(int $scale, int $value, float $expected): void
    {
        static::assertSame($expected, (new DecimalConverter(18, $scale))->fromParquetType($value));
    }

    #[TestWith([9, 2, 12345.67, 1234567])]
    #[TestWith([9, 2, 0.125, 13])]
    public function test_to_parquet_type(int $precision, int $scale, float $value, int $expected): void
    {
        static::assertSame($expected, (new DecimalConverter($precision, $scale))->toParquetType($value));
    }

    public function test_to_parquet_type_over_precision_throws(): void
    {
        $this->expectException(OverflowException::class);

        (new DecimalConverter(9, 2))->toParquetType(12345678.9);
    }
}
