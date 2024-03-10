<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BinaryReader;

use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\{ByteOrder, DataSize};
use PHPUnit\Framework\TestCase;

final class BytesTest extends TestCase
{
    public function test_buffer_from_string() : void
    {
        $buffer = Bytes::fromString('Hello');
        self::assertEquals('Hello', $buffer->toString());
        self::assertEquals(new DataSize(40), $buffer->size());
    }

    public function test_buffer_size() : void
    {
        $buffer = new Bytes([72, 101, 108, 108, 111]);  // ASCII for "Hello"
        self::assertEquals(new DataSize(40), $buffer->size());  // 5 bytes * 8 bits per byte = 40 bits
    }

    public function test_buffer_to_string() : void
    {
        $buffer = new Bytes([72, 101, 108, 108, 111]);  // ASCII for "Hello"
        self::assertEquals('Hello', $buffer->toString());
    }

    public function test_empty_buffer_size() : void
    {
        $buffer = new Bytes([]);
        self::assertEquals(new DataSize(0), $buffer->size());
    }

    public function test_empty_buffer_to_string() : void
    {
        $buffer = new Bytes([]);
        self::assertEquals('', $buffer->toString());
    }

    public function test_to_int() : void
    {
        $bytes = new Bytes([1, 0, 0, 0]);  // Little-endian representation of the integer 1
        self::assertEquals(1, $bytes->toInt());
    }

    public function test_to_int_big_endian() : void
    {
        $bytes = new Bytes([0, 0, 0, 1], ByteOrder::BIG_ENDIAN);
        self::assertEquals(1, $bytes->toInt());
    }

    public function test_to_int_big_endian_multi_byte() : void
    {
        $bytes = new Bytes([1, 0, 0], ByteOrder::BIG_ENDIAN);  // Big-endian representation of the integer 65536
        self::assertEquals(65536, $bytes->toInt());
    }

    public function test_to_int_big_endian_non_zero_bytes() : void
    {
        $bytes = new Bytes([0xFF, 0xFF], ByteOrder::BIG_ENDIAN);
        self::assertEquals(65535, $bytes->toInt());
    }

    public function test_to_int_empty_bytes() : void
    {
        $bytes = new Bytes([]);  // Should return 0 when bytes array is empty
        self::assertEquals(0, $bytes->toInt());
    }

    public function test_to_int_non_zero_bytes() : void
    {
        $bytes = new Bytes([0xFF, 0xFF]);  // Little-endian representation of the integer 65535
        self::assertEquals(65535, $bytes->toInt());
    }

    public function test_to_int_single_byte() : void
    {
        $bytes = new Bytes([42]);  // Single byte representing the integer 42
        self::assertEquals(42, $bytes->toInt());
    }
}
