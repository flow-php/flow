<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BinaryReader;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ByteOrder;
use PHPUnit\Framework\TestCase;

final class BinaryBufferReaderTest extends TestCase
{
    public function test_readDouble() : void
    {
        $buffer = \pack('C8', 0x40, 0x45, 0x6E, 0x97, 0xF7, 0x9A, 0xD9, 0xF4);
        $reader = new BinaryBufferReader($buffer, ByteOrder::BIG_ENDIAN);
        $result = $reader->readDouble();
        $this->assertEqualsWithDelta(42.8640126711, $result, 0.0000000001);
    }

    public function test_readFloat() : void
    {
        $buffer = \pack('C4', 0x42, 0x29, 0xB9, 0x9A);
        $reader = new BinaryBufferReader($buffer, ByteOrder::BIG_ENDIAN);
        $result = $reader->readFloat();
        $this->assertEqualsWithDelta(42.43125152587, $result, 0.0001);
    }

    public function test_readInt32_big_endian() : void
    {
        $buffer = \pack('C*', 0, 0, 0, 1);  // 0x00000001 in big endian
        $reader = new BinaryBufferReader($buffer, ByteOrder::BIG_ENDIAN);

        $result = $reader->readInt32();
        $this->assertSame(1, $result);
    }

    public function test_readInt32_boundary() : void
    {
        $buffer = \pack('C*', 255, 255, 255, 255);  // 0xFFFFFFFF
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readInt32();
        $this->assertSame(4294967295, $result);  // PHP treats it as unsigned here
    }

    public function test_readInt32_little_endian() : void
    {
        $buffer = \pack('C*', 1, 0, 0, 0);  // 0x00000001 in little endian
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readInt32();
        $this->assertSame(1, $result);
    }

    public function test_readInt64() : void
    {
        $buffer = \pack('C*', 1, 0, 0, 0, 0, 0, 0, 0);
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readInt64();
        $this->assertSame(1, $result);
    }

    public function test_readString_valid_input() : void
    {
        $stringData = 'Hello, World!';
        $length = \pack('V', \strlen($stringData)); // Assuming little-endian 32-bit length
        $buffer = $length . $stringData;

        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readString();

        $this->assertSame($stringData, $result);
    }

    public function test_readString_zero_length() : void
    {
        $length = \pack('V', 0); // 32-bit zero length
        $buffer = $length;

        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readString();

        $this->assertSame('', $result);
    }

    public function test_readUInt32() : void
    {
        $buffer = \pack('C*', 1, 0, 0, 0);
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readUInt32();
        $this->assertSame(1, $result);
    }

    public function test_readUInt64() : void
    {
        $buffer = \pack('C*', 1, 0, 0, 0, 0, 0, 0, 0);
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        $result = $reader->readUInt64();
        $this->assertSame(1, $result);  // This test assumes PHP handles unsigned 64-bit integers
    }

    public function test_readVarInt() : void
    {
        // Using examples:
        // 1 is encoded as 00000001
        // 300 is encoded as 10101100 00000010
        $buffer = \pack('C*', 0x01, 0xAC, 0x02);
        $reader = new BinaryBufferReader($buffer, ByteOrder::LITTLE_ENDIAN);

        // First varint should be 1
        $result1 = $reader->readVarInt();
        $this->assertSame(1, $result1);

        // Second varint should be 300
        $result2 = $reader->readVarInt();
        $this->assertSame(300, $result2);
    }
}
