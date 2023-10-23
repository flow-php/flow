<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\BinaryReader;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\ByteOrder;
use PHPUnit\Framework\TestCase;

final class BinaryBufferReaderTest extends TestCase
{
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
