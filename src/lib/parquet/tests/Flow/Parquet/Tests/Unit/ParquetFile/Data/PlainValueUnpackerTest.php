<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\{BinaryReader, Option, Options};
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Data\PlainValueUnpacker;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use PHPUnit\Framework\TestCase;

final class PlainValueUnpackerTest extends TestCase
{
    public function test_unpack_boolean() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::boolean('test_column');

        $binaryReader->expects(self::once())
            ->method('readBooleans')
            ->with(2)
            ->willReturn((static function () {
                yield true;
                yield false;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([true, false], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_default_with_byte_array_to_string_option_false() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);

        $bytes1 = new Bytes([1, 2, 3]);
        $bytes2 = new Bytes([4, 5, 6]);

        $binaryReader->expects(self::once())
            ->method('readByteArrays')
            ->with(2)
            ->willReturn((static function () use ($bytes1, $bytes2) {
                yield $bytes1;
                yield $bytes2;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, (new Options())->set(Option::BYTE_ARRAY_TO_STRING, false));

        self::assertEquals([$bytes1, $bytes2], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_default_with_byte_array_to_string_option_true() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY);

        $binaryReader->expects(self::once())
            ->method('readStrings')
            ->with(2)
            ->willReturn((static function () {
                yield 'string1';
                yield 'string2';
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, (new Options())->set(Option::BYTE_ARRAY_TO_STRING, true));

        self::assertEquals(['string1', 'string2'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_json_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::json('test_column');

        $binaryReader->expects(self::once())
            ->method('readStrings')
            ->with(2)
            ->willReturn((static function () {
                yield '{"key": "value"}';
                yield '{"foo": "bar"}';
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals(['{"key": "value"}', '{"foo": "bar"}'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_string_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::string('test_column');

        $binaryReader->expects(self::once())
            ->method('readStrings')
            ->with(2)
            ->willReturn((static function () {
                yield 'hello';
                yield 'world';
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals(['hello', 'world'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_byte_array_with_uuid_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = new FlatColumn('test_column', PhysicalType::BYTE_ARRAY, null, LogicalType::uuid());

        $binaryReader->expects(self::once())
            ->method('readStrings')
            ->with(2)
            ->willReturn((static function () {
                yield 'uuid1';
                yield 'uuid2';
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals(['uuid1', 'uuid2'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_double() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::double('test_column');

        $binaryReader->expects(self::once())
            ->method('readDoubles')
            ->with(2)
            ->willReturn((static function () {
                yield 1.123456789;
                yield 2.987654321;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([1.123456789, 2.987654321], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_fixed_len_byte_array_with_decimal_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::decimal('test_column', 10, 2);

        $binaryReader->expects(self::once())
            ->method('readDecimals')
            ->with(2, $column->typeLength(), 10, 2)
            ->willReturn((static function () {
                yield 123.45;
                yield 678.90;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([123.45, 678.90], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_fixed_len_byte_array_with_null_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = new FlatColumn('test_column', PhysicalType::FIXED_LEN_BYTE_ARRAY, null, null, null, null, null, 16);

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported logical type null for FIXED_LEN_BYTE_ARRAY');

        iterator_to_array($unpacker->unpack($column, 2));
    }

    public function test_unpack_fixed_len_byte_array_with_unsupported_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = new FlatColumn('test_column', PhysicalType::FIXED_LEN_BYTE_ARRAY, null, LogicalType::string(), null, null, null, 16);

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        $this->expectException(RuntimeException::class);
        $this->expectExceptionMessage('Unsupported logical type STRING for FIXED_LEN_BYTE_ARRAY');

        iterator_to_array($unpacker->unpack($column, 2));
    }

    public function test_unpack_fixed_len_byte_array_with_uuid_logical_type() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::uuid('test_column');

        $binaryReader->expects(self::once())
            ->method('readStrings')
            ->with(2)
            ->willReturn((static function () {
                yield 'fixed-uuid1';
                yield 'fixed-uuid2';
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals(['fixed-uuid1', 'fixed-uuid2'], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_float() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::float('test_column');

        $binaryReader->expects(self::once())
            ->method('readFloats')
            ->with(2)
            ->willReturn((static function () {
                yield 1.5;
                yield 2.5;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([1.5, 2.5], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_int32_default() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::int32('test_column');

        $binaryReader->expects(self::once())
            ->method('readInts32')
            ->with(2)
            ->willReturn((static function () {
                yield 100;
                yield 200;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([100, 200], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_int64() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::int64('test_column');

        $binaryReader->expects(self::once())
            ->method('readInts64')
            ->with(2)
            ->willReturn((static function () {
                yield 1000;
                yield 2000;
            })());

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEquals([1000, 2000], iterator_to_array($unpacker->unpack($column, 2)));
    }

    public function test_unpack_with_zero_total_returns_empty_generator() : void
    {
        $binaryReader = $this->createMock(BinaryReader::class);
        $column = FlatColumn::int32('test_column');

        $unpacker = new PlainValueUnpacker($binaryReader, new Options());

        self::assertEmpty(iterator_to_array($unpacker->unpack($column, 0)));
    }
}
