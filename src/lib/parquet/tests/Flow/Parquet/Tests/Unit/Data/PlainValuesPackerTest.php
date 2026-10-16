<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\PlainValuesPacker;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter\DecimalConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Time;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\Repetition;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;
use PHPUnit\Framework\TestCase;

use function iterator_to_array;

final class PlainValuesPackerTest extends TestCase
{
    public function test_byte_array_decimal_round_trip(): void
    {
        $column = new FlatColumn('d', PhysicalType::BYTE_ARRAY, logicalType: LogicalType::decimal(2, 9));
        $converter = DecimalConverter::forColumn($column, Options::default());
        static::assertNotNull($converter);
        $physical = [$converter->toParquetType(12345.67), $converter->toParquetType(-12345.67)];

        $buffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($buffer)))->packValues($column, $physical);
        $unpacked = iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($buffer)))->unpack($column, 2));

        static::assertSame(["\x12\xD6\x87", "\xED\x29\x79"], $unpacked);
        static::assertSame([12345.67, -12345.67], [
            $converter->fromParquetType($unpacked[0]),
            $converter->fromParquetType($unpacked[1]),
        ]);
    }

    public function test_fixed_len_byte_array_decimal_above_int64_round_trip(): void
    {
        $column = new FlatColumn(
            'd',
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            null,
            LogicalType::decimal(10, 50),
            Repetition::OPTIONAL,
            typeLength: 21,
        );
        $converter = DecimalConverter::forColumn($column, Options::default());
        static::assertNotNull($converter);
        $physical = [$converter->toParquetType(1.2345678901234568E+39), $converter->toParquetType(-12345.67)];

        $buffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($buffer)))->packValues($column, $physical);
        $unpacked = iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($buffer)))->unpack($column, 2));

        static::assertSame($physical, $unpacked);
        static::assertSame([1.2345678901234568E+39, -12345.67], [
            $converter->fromParquetType($unpacked[0]),
            $converter->fromParquetType($unpacked[1]),
        ]);
    }

    public function test_int32_decimal_round_trip(): void
    {
        $column = new FlatColumn('d', PhysicalType::INT32, logicalType: LogicalType::decimal(2, 9));

        $buffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($buffer)))->packValues($column, [1234567]);

        static::assertSame(
            [1234567],
            iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($buffer)))->unpack($column, 1)),
        );
    }

    public function test_int32_time_round_trip(): void
    {
        $column = new FlatColumn(
            't',
            PhysicalType::INT32,
            logicalType: new LogicalType(LogicalType::TIME, time: new Time(false, TimeUnit::MILLISECONDS)),
        );

        $buffer = '';
        (new PlainValuesPacker(new BinaryBufferWriter($buffer)))->packValues($column, [11045678]);

        static::assertSame(
            [11045678],
            iterator_to_array((new PlainValueUnpacker(new BinaryBufferReader($buffer)))->unpack($column, 1)),
        );
    }
}
