<?php declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\DataSize;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use PHPUnit\Framework\TestCase;

final class RLEBitPackedHybridTest extends TestCase
{
    public function test_decodeBitPacked_with_byteCount_greater_than_rawBytes_length() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects($this->once())
            ->method('remainingLength')
            ->willReturn(new DataSize(1));

        $binaryReader->expects($this->once())
            ->method('readBytes')
            ->willReturn(new Bytes([8]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        $this->assertEquals([8], $result);
    }

    public function test_decodeBitPacked_with_different_bitWidth() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 4;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects($this->once())
            ->method('remainingLength')
            ->willReturn(new DataSize(2));

        $binaryReader->expects($this->once())
            ->method('readBytes')
            ->willReturn(new Bytes([8, 4]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        $this->assertEquals([8, 0, 4, 0], $result);
    }

    public function test_decodeBitPacked_with_fewer_remainingBytes_than_byteCount() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects($this->once())
            ->method('remainingLength')
            ->willReturn(new DataSize(1));

        $binaryReader->expects($this->once())
            ->method('readBytes')
            ->willReturn(new Bytes([8]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        $this->assertEquals([8], $result);
    }

    public function test_decodeBitPacked_with_zero_groupCount_and_count() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $varInt = 0;
        $maxItems = 5;

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        $this->assertEquals([], $result);
    }

    public function test_decodeRLE_with_isLiteralRun_false() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $intVar = 4; // Even intVar, so isLiteralRun will be false
        $maxItems = 2;

        $binaryReader->expects($this->once())
            ->method('readBytes')
            ->willReturn(new Bytes([2]));

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        $this->assertEquals([2, 2], $result);
    }

    public function test_decodeRLE_with_isLiteralRun_true() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $intVar = 3; // Odd intVar, so isLiteralRun will be true
        $maxItems = 2;

        $binaryReader->expects($this->exactly(1))
            ->method('readBytes')
            ->willReturn(new Bytes([]));

        $binaryReader->expects($this->exactly(1))
            ->method('readBits')
            ->willReturnOnConsecutiveCalls([1]);

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        $this->assertEquals([[1]], $result);
    }

    public function test_decodeRLE_with_runLength_zero() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $intVar = 0;
        $maxItems = 5;

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        $this->assertEquals([], $result);
    }
}
