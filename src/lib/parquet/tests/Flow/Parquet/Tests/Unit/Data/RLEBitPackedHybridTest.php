<?php

declare(strict_types=1);

namespace Flow\Parquet\Tests\Unit\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\Data\RLEBitPackedHybrid;
use Flow\Parquet\DataSize;
use PHPUnit\Framework\TestCase;

use function chr;

final class RLEBitPackedHybridTest extends TestCase
{
    public function test_decode_bit_packed_with_byte_count_greater_than_raw_bytes_length(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)->disableOriginalConstructor()->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())->method('remainingLength')->willReturn(new DataSize(1));

        $binaryReader->expects(self::once())->method('readBytes')->willReturn(chr(8));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        static::assertEquals([8], $result);
    }

    public function test_decode_bit_packed_with_different_bit_width(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)->disableOriginalConstructor()->getMock();

        $bitWidth = 4;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())->method('remainingLength')->willReturn(new DataSize(2));

        $binaryReader
            ->expects(self::once())
            ->method('readBytes')
            ->willReturn(chr(8) . chr(4));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        static::assertEquals([8, 0, 4, 0], $result);
    }

    public function test_decode_bit_packed_with_fewer_remaining_bytes_than_byte_count(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)->disableOriginalConstructor()->getMock();

        $bitWidth = 8;
        $varInt = 2;
        $maxItems = 5;

        $binaryReader->expects(self::once())->method('remainingLength')->willReturn(new DataSize(1));

        $binaryReader->expects(self::once())->method('readBytes')->willReturn(chr(8));

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        static::assertEquals([8], $result);
    }

    public function test_decode_bit_packed_with_zero_group_count_and_count(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->createStub(BinaryReader::class);

        $bitWidth = 8;
        $varInt = 0;
        $maxItems = 5;

        $result = [];
        $rleBitPackedHybrid->decodeBitPacked($binaryReader, $bitWidth, $varInt, $maxItems, $result);

        static::assertEquals([], $result);
    }

    public function test_decode_rl_e_with_is_literal_run_false(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->getMockBuilder(BinaryReader::class)->disableOriginalConstructor()->getMock();

        $bitWidth = 8;
        $intVar = 4; // Even intVar, so isLiteralRun will be false
        $maxItems = 2;

        $binaryReader->expects(self::once())->method('readBytes')->willReturn(chr(2));

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        static::assertEquals([2, 2], $result);
    }

    public function test_decode_rl_e_with_run_length_zero(): void
    {
        $rleBitPackedHybrid = new RLEBitPackedHybrid();
        $binaryReader = $this->createStub(BinaryReader::class);

        $bitWidth = 8;
        $intVar = 0;
        $maxItems = 5;

        $result = [];
        $rleBitPackedHybrid->decodeRLE($binaryReader, $bitWidth, $intVar, $maxItems, $result);

        static::assertEquals([0], $result);
    }
}
