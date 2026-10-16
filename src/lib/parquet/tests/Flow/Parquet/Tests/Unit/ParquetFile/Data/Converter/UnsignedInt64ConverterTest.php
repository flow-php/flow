<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use PHPUnit\Framework\Attributes\TestWith;
use PHPUnit\Framework\TestCase;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

final class UnsignedInt64ConverterTest extends TestCase
{
    #[TestWith([0, '0'])]
    #[TestWith([PHP_INT_MAX, '9223372036854775807'])]
    #[TestWith([PHP_INT_MIN, '9223372036854775808'])]
    #[TestWith([-1, '18446744073709551615'])]
    public function test_bit_pattern_reads_as_its_decimal_string_and_back(int $physical, string $logical): void
    {
        $converter = new UnsignedInt64Converter();

        static::assertSame($logical, $converter->fromParquetType($physical));
        static::assertSame($physical, $converter->toParquetType($logical));
    }

    public function test_an_int_writes_as_itself(): void
    {
        static::assertSame(PHP_INT_MAX, (new UnsignedInt64Converter())->toParquetType(PHP_INT_MAX));
    }

    #[TestWith([-1, '-1'])]
    #[TestWith(['abc', 'abc'])]
    #[TestWith(['18446744073709551616', '18446744073709551616'])]
    #[TestWith(['99999999999999999999', '99999999999999999999'])]
    #[TestWith(['007', '007'])]
    #[TestWith(['', ''])]
    #[TestWith([1.5, 'float'])]
    public function test_a_value_out_of_uint64_is_refused(mixed $value, string $shown): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected an int >= 0 or a digit string up to 18446744073709551615, got '
        . $shown);

        (new UnsignedInt64Converter())->toParquetType($value);
    }

    public function test_reading_anything_but_an_int_is_refused(): void
    {
        $this->expectException(InvalidArgumentException::class);
        $this->expectExceptionMessage('Expected int, got string');

        (new UnsignedInt64Converter())->fromParquetType('1');
    }

    public function test_for_column_takes_converted_and_logical_uint64_only(): void
    {
        static::assertNotNull(UnsignedInt64Converter::forColumn(
            new FlatColumn('u', PhysicalType::INT64, ConvertedType::UINT_64),
            Options::default(),
        ));
        static::assertNotNull(UnsignedInt64Converter::forColumn(
            new FlatColumn('u', PhysicalType::INT64, logicalType: LogicalType::integer(64, false)),
            Options::default(),
        ));
        static::assertNull(UnsignedInt64Converter::forColumn(
            new FlatColumn('u', PhysicalType::INT32, ConvertedType::UINT_32),
            Options::default(),
        ));
        static::assertNull(UnsignedInt64Converter::forColumn(FlatColumn::int64('i'), Options::default()));
    }
}
