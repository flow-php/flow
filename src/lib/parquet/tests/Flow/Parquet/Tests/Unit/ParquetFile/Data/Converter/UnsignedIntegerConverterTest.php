<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedIntegerConverter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

final class UnsignedIntegerConverterTest extends TestCase
{
    #[TestWith([-294_967_296, 4_000_000_000])]
    #[TestWith([-1, 4_294_967_295])]
    #[TestWith([0, 0])]
    #[TestWith([2_147_483_647, 2_147_483_647])]
    public function test_uint32_reads_back_as_itself(int $stored, int $value): void
    {
        static::assertSame($value, (new UnsignedIntegerConverter('u32', 32))->fromParquetType($stored));
    }

    public function test_uint64_up_to_php_int_max_reads_as_is(): void
    {
        static::assertSame(PHP_INT_MAX, (new UnsignedIntegerConverter('u64', 64))->fromParquetType(PHP_INT_MAX));
    }

    public function test_uint64_above_php_int_max_is_refused(): void
    {
        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Parquet column "u64" holds a UINT_64 value above PHP_INT_MAX');

        (new UnsignedIntegerConverter('u64', 64))->fromParquetType(-1);
    }

    public function test_a_value_that_is_not_an_int_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected int, got string');

        (new UnsignedIntegerConverter('u32', 32))->fromParquetType('1');
    }

    public function test_writes_are_the_value_itself(): void
    {
        static::assertSame(4_000_000_000, (new UnsignedIntegerConverter('u32', 32))->toParquetType(4_000_000_000));
    }

    #[TestWith([ConvertedType::UINT_32, 32])]
    #[TestWith([ConvertedType::UINT_64, 64])]
    public function test_for_column_reads_the_converted_type(ConvertedType $convertedType, int $bitWidth): void
    {
        static::assertEquals(
            new UnsignedIntegerConverter('u', $bitWidth),
            UnsignedIntegerConverter::forColumn(
                new FlatColumn('u', PhysicalType::INT64, $convertedType),
                Options::default(),
            ),
        );
    }

    public function test_for_column_reads_a_logical_unsigned_integer(): void
    {
        static::assertEquals(
            new UnsignedIntegerConverter('u', 32),
            UnsignedIntegerConverter::forColumn(
                new FlatColumn('u', PhysicalType::INT32, logicalType: LogicalType::integer(32, false)),
                Options::default(),
            ),
        );
    }

    #[TestWith([ConvertedType::UINT_8])]
    #[TestWith([ConvertedType::UINT_16])]
    #[TestWith([ConvertedType::INT_32])]
    #[TestWith([ConvertedType::INT_64])]
    public function test_for_column_skips_what_a_signed_int_holds(ConvertedType $convertedType): void
    {
        static::assertNull(UnsignedIntegerConverter::forColumn(
            new FlatColumn('u', PhysicalType::INT32, $convertedType),
            Options::default(),
        ));
        static::assertNull(UnsignedIntegerConverter::forColumn(FlatColumn::int64('i'), Options::default()));
    }
}
