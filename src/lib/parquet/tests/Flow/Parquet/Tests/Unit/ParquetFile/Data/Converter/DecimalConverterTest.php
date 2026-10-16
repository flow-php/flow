<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
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
    public function test_for_column_ignores_columns_without_decimal(): void
    {
        static::assertNull(DecimalConverter::forColumn(FlatColumn::int64('i'), Options::default()));
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
        static::assertSame(1234567, $converter->toParquetType(12345.67));
    }

    public function test_for_column_on_fixed_len_byte_array_decimals_uses_the_type_length(): void
    {
        $converter = DecimalConverter::forColumn(FlatColumn::decimal('d', 20, 2), Options::default());

        static::assertNotNull($converter);
        static::assertSame("\x00\x00\x00\x00\x00\x00\x00\x00\x01", $converter->toParquetType(0.01));
        static::assertSame("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xA2", $converter->toParquetType(-3.5));
        static::assertSame(-3.5, $converter->fromParquetType("\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFE\xA2"));
    }

    public function test_for_column_on_byte_array_decimals_uses_minimal_bytes(): void
    {
        $converter = DecimalConverter::forColumn(
            new FlatColumn('d', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::decimal(2, 9)),
            Options::default(),
        );

        static::assertNotNull($converter);
        static::assertSame("\xED\x29\x79", $converter->toParquetType(-12345.67));
        static::assertSame(-12345.67, $converter->fromParquetType("\xED\x29\x79"));
    }

    #[TestWith([2, 1234567, 12345.67])]
    #[TestWith([2, -1, -0.01])]
    #[TestWith([0, 42, 42.0])]
    #[TestWith([2, 123456789012345678, 1234567890123456.8])]
    public function test_from_parquet_type(int $scale, int $value, float $expected): void
    {
        static::assertSame($expected, (new DecimalConverter(PhysicalType::INT64, 18, $scale))->fromParquetType($value));
    }

    #[TestWith([9, 2, 12345.67, 1234567])]
    #[TestWith([9, 2, 0.125, 13])]
    public function test_to_parquet_type(int $precision, int $scale, float $value, int $expected): void
    {
        static::assertSame(
            $expected,
            (new DecimalConverter(PhysicalType::INT32, $precision, $scale))->toParquetType($value),
        );
    }

    public function test_to_parquet_type_over_precision_throws(): void
    {
        $this->expectException(OverflowException::class);

        (new DecimalConverter(PhysicalType::INT32, 9, 2))->toParquetType(12345678.9);
    }

    #[TestWith([true])]
    #[TestWith(['1.25'])]
    public function test_to_parquet_type_refuses_a_non_number(mixed $value): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Decimal must be written as int or float, got ');

        (new DecimalConverter(PhysicalType::INT64, 10, 2))->toParquetType($value);
    }
}
