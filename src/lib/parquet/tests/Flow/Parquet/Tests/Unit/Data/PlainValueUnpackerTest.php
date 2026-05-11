<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use PHPUnit\Framework\TestCase;

use function Flow\Parquet\Binary\encode_decimal;
use function Flow\Parquet\Binary\encode_f32;
use function Flow\Parquet\Binary\encode_f64;
use function Flow\Parquet\Binary\encode_i32;
use function Flow\Parquet\Binary\encode_i64;
use function Flow\Parquet\Binary\encode_u32;

final class PlainValueUnpackerTest extends TestCase
{
    public function test_unpack_boolean(): void
    {
        $buffer = \chr(0b00000010);
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::boolean('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([false, true], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_default_with_byte_array_to_string_option_false(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $string1 = 'abc';
        $string2 = 'def';
        $buffer =
            encode_u32($byteOrder, [\strlen($string1)])
            . $string1
            . encode_u32($byteOrder, [\strlen($string2)])
            . $string2;
        $reader = new BinaryBufferReader($buffer);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);

        $unpacker = new PlainValueUnpacker($reader);

        $result = iterator_to_array($unpacker->unpack($column, 2));
        static::assertCount(2, $result);
        static::assertIsString($result[0]);
        static::assertIsString($result[1]);
        static::assertEquals('abc', $result[0]);
        static::assertEquals('def', $result[1]);
    }

    public function test_unpack_byte_array_default_with_byte_array_to_string_option_true(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $string1 = 'string1';
        $string2 = 'string2';
        $buffer =
            encode_u32($byteOrder, [\strlen($string1)])
            . $string1
            . encode_u32($byteOrder, [\strlen($string2)])
            . $string2;
        $reader = new BinaryBufferReader($buffer);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals(['string1', 'string2'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_json_logical_type(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $json1 = '{"key": "value"}';
        $json2 = '{"foo": "bar"}';
        $buffer =
            encode_u32($byteOrder, [\strlen($json1)]) . $json1 . encode_u32($byteOrder, [\strlen($json2)]) . $json2;
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::json('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals(['{"key": "value"}', '{"foo": "bar"}'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_string_logical_type(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $string1 = 'hello';
        $string2 = 'world';
        $buffer =
            encode_u32($byteOrder, [\strlen($string1)])
            . $string1
            . encode_u32($byteOrder, [\strlen($string2)])
            . $string2;
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::string('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals(['hello', 'world'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_uuid_logical_type(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $uuid1 = 'uuid1';
        $uuid2 = 'uuid2';
        $buffer =
            encode_u32($byteOrder, [\strlen($uuid1)]) . $uuid1 . encode_u32($byteOrder, [\strlen($uuid2)]) . $uuid2;
        $reader = new BinaryBufferReader($buffer);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY, null, LogicalType::uuid());

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals(['uuid1', 'uuid2'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_double(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = encode_f64($byteOrder, [1.123456789]) . encode_f64($byteOrder, [2.987654321]);
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::double('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([1.123456789, 2.987654321], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_fixed_len_byte_array_with_decimal_logical_type(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $column = FlatColumn::decimal('test_column', 10, 2);
        $buffer = encode_decimal($byteOrder, 123.45, $column->typeLength(), 10, 2);
        $buffer .= encode_decimal($byteOrder, 678.90, $column->typeLength(), 10, 2);
        $reader = new BinaryBufferReader($buffer);

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([123.45, 678.90], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_fixed_len_byte_array_with_null_logical_type_returns_raw_string(): void
    {
        $data1 = \str_repeat('A', 16);
        $data2 = \str_repeat('B', 16);
        $buffer = $data1 . $data2;
        $reader = new BinaryBufferReader($buffer);
        $column = new FlatColumn('test_column', PhysicalType::FIXED_LEN_BYTE_ARRAY, null, null, null, null, null, 16);

        $unpacker = new PlainValueUnpacker($reader);

        $result = iterator_to_array($unpacker->unpack($column, 2));
        static::assertCount(2, $result);
        static::assertIsString($result[0]);
        static::assertIsString($result[1]);
        static::assertEquals($data1, $result[0]);
        static::assertEquals($data2, $result[1]);
    }

    public function test_unpack_fixed_len_byte_array_with_unsupported_logical_type_returns_raw_string(): void
    {
        $data1 = \str_repeat('X', 16);
        $data2 = \str_repeat('Y', 16);
        $buffer = $data1 . $data2;
        $reader = new BinaryBufferReader($buffer);
        $column = new FlatColumn(
            'test_column',
            PhysicalType::FIXED_LEN_BYTE_ARRAY,
            null,
            LogicalType::string(),
            null,
            null,
            null,
            16,
        );

        $unpacker = new PlainValueUnpacker($reader);

        $result = iterator_to_array($unpacker->unpack($column, 2));
        static::assertCount(2, $result);
        static::assertIsString($result[0]);
        static::assertIsString($result[1]);
        static::assertEquals($data1, $result[0]);
        static::assertEquals($data2, $result[1]);
    }

    public function test_unpack_fixed_len_byte_array_with_uuid_logical_type(): void
    {
        $uuid1 = \hex2bin('550e8400e29b41d4a716446655440000');
        $uuid2 = \hex2bin('6ba7b8109dad11d180b400c04fd430c8');
        $buffer = $uuid1 . $uuid2;
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::uuid('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        $result = iterator_to_array($unpacker->unpack($column, 2));
        static::assertCount(2, $result);
        static::assertEquals('550e8400-e29b-41d4-a716-446655440000', $result[0]);
        static::assertEquals('6ba7b810-9dad-11d1-80b4-00c04fd430c8', $result[1]);
    }

    public function test_unpack_float(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = encode_f32($byteOrder, [1.5]) . encode_f32($byteOrder, [2.5]);
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::float('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([1.5, 2.5], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_int32_default(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = encode_i32($byteOrder, [100]) . encode_i32($byteOrder, [200]);
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::int32('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([100, 200], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_int64(): void
    {
        $byteOrder = ByteOrder::LITTLE_ENDIAN;
        $buffer = encode_i64($byteOrder, [1000]) . encode_i64($byteOrder, [2000]);
        $reader = new BinaryBufferReader($buffer);
        $column = FlatColumn::int64('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEquals([1000, 2000], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_with_zero_total_returns_empty_generator(): void
    {
        $reader = new BinaryBufferReader('');
        $column = FlatColumn::int32('test_column');

        $unpacker = new PlainValueUnpacker($reader);

        static::assertEmpty(iterator_to_array($unpacker->unpack($column, 0)));
    }
}
