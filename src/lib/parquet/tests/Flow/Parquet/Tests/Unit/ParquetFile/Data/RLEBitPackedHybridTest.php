<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\ParquetFile\Data;

use Flow\Parquet\BinaryReader\Bytes;
use Flow\Parquet\ParquetFile\Data\RLEBitPackedHybrid;
use Flow\Parquet\{BinaryReader, DataSize};
use PHPUnit\Framework\TestCase;

final class RLEBitPackedHybridTest extends TestCase
{
    public function test_decode_bit_packed_with_byte_count_greater_than_raw_bytes_length() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())
            ->method('remainingLength')
            ->willReturn(new DataSize(1));

        $binaryReader->expects(self::once())
            ->method('readBytes')
            ->willReturn(new Bytes([8]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        self::assertEquals([8], $result);
    }

    public function test_decode_bit_packed_with_different_bit_width() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 4;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())
            ->method('remainingLength')
            ->willReturn(new DataSize(2));

        $binaryReader->expects(self::once())
            ->method('readBytes')
            ->willReturn(new Bytes([8, 4]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        self::assertEquals([8, 0, 4, 0], $result);
    }

    public function test_decode_bit_packed_with_fewer_remaining_bytes_than_byte_count() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())
            ->method('remainingLength')
            ->willReturn(new DataSize(1));

        $binaryReader->expects(self::once())
            ->method('readBytes')
            ->willReturn(new Bytes([8]));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        self::assertEquals([8], $result);
    }

    public function test_decode_bit_packed_with_zero_group_count_and_count() : void
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

        self::assertEquals([], $result);
    }

    public function test_decode_rl_e_with_is_literal_run_false() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $intVar = 4; // Even intVar, so isLiteralRun will be false
        $maxItems = 2;

        $binaryReader->expects(self::once())
            ->method('readBytes')
            ->willReturn(new Bytes([2]));

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        self::assertEquals([2, 2], $result);
    }

    public function test_decode_rl_e_with_is_literal_run_true() : void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)
            ->disableOriginalConstructor()
            ->getMock();

        $bitWidth = 8;
        $intVar = 3; // Odd intVar, so isLiteralRun will be true
        $maxItems = 2;

        $binaryReader->expects(self::exactly(1))
            ->method('readBytes')
            ->willReturn(new Bytes([]));

        $binaryReader->expects(self::exactly(1))
            ->method('readBits')
            ->willReturnOnConsecutiveCalls([1]);

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        self::assertEquals([[1]], $result);
    }

    public function test_decode_rl_e_with_run_length_zero() : void
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

        self::assertEquals([0], $result);
    }
}
