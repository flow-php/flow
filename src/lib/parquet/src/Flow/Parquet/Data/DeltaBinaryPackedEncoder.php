<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function array_fill;
use function array_map;
use function array_pad;
use function array_slice;
use function ceil;
use function chr;
use function count;
use function intdiv;
use function max;
use function min;
use function pack;
use function str_pad;

final readonly class DeltaBinaryPackedEncoder
{
    private const int DEFAULT_BLOCK_SIZE = 128;

    private const int DEFAULT_MINIBLOCK_SIZE = 32;

    public function __construct(
        private int $blockSize = self::DEFAULT_BLOCK_SIZE,
        private int $miniblockSize = self::DEFAULT_MINIBLOCK_SIZE,
        private DeltaCalculator $deltaCalculator = new DeltaCalculator(),
        private ZigZag $zigzag = new ZigZag(),
        private PhysicalType $physicalType = PhysicalType::INT64,
    ) {
        if (($this->blockSize % 128) !== 0) {
            throw new InvalidArgumentException('Block size must be a multiple of 128');
        }

        if (($this->miniblockSize % 32) !== 0) {
            throw new InvalidArgumentException('Miniblock size must be a multiple of 32');
        }

        if (($this->blockSize % $this->miniblockSize) !== 0) {
            throw new InvalidArgumentException('Block size must be a multiple of miniblock size');
        }
    }

    /**
     * @param array<int> $values
     */
    public function encode(array $values): string
    {
        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        $this->writeHeader($writer, $values);
        $this->writeBlocks($writer, $values);

        return $buffer;
    }

    /**
     * @param array<int> $values
     */
    private function packMiniblock(array $values, int $bitWidth): string
    {
        // a zero-width miniblock has no body: its bit width alone says every delta equals the block's minimum
        if ($bitWidth === 0) {
            return '';
        }

        if ($bitWidth > BitWidth::MAX_PACKED_IN_ACCUMULATOR) {
            return $this->packMiniblockSafe($values, $bitWidth);
        }

        $buffer = '';
        $currentByte = 0;
        $bitsInByte = 0;

        foreach ($values as $value) {
            $currentByte |= $value << $bitsInByte;
            $bitsInByte += $bitWidth;

            while ($bitsInByte >= 8) {
                $buffer .= chr($currentByte & 0xFF);
                $currentByte >>= 8;
                $bitsInByte -= 8;
            }
        }

        if ($bitsInByte > 0) {
            $buffer .= chr($currentByte & 0xFF);
        }

        return str_pad($buffer, (int) ceil(($this->miniblockSize * $bitWidth) / 8), "\0");
    }

    /**
     * Packs bytes manually so a large bit width cannot overflow an integer.
     *
     * @param array<int> $values
     */
    private function packMiniblockSafe(array $values, int $bitWidth): string
    {
        $expectedByteCount = (int) ceil(($this->miniblockSize * $bitWidth) / 8);
        $bytes = array_fill(0, max(0, $expectedByteCount), 0);

        $globalBitOffset = 0;

        foreach ($values as $value) {
            // Pack this value bit by bit
            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $bitValue = ($value >> $bit) & 1;

                if ($bitValue) {
                    $byteIndex = intdiv($globalBitOffset, 8);
                    $bitIndex = $globalBitOffset % 8;

                    if ($byteIndex < $expectedByteCount) {
                        $bytes[$byteIndex] |= 1 << $bitIndex;
                    }
                }

                $globalBitOffset++;
            }
        }

        return pack('C*', ...$bytes);
    }

    /**
     * @param array<int> $miniblockDeltas
     *
     * @return array<int>
     */
    private function padMiniblock(array $miniblockDeltas): array
    {
        return array_pad($miniblockDeltas, $this->miniblockSize, 0);
    }

    /**
     * @param array<int> $blockDeltas
     */
    private function writeBlock(BinaryBufferWriter $writer, array $blockDeltas): void
    {
        if (!count($blockDeltas)) {
            return;
        }

        $minDelta = min($blockDeltas);
        $this->writeSignedLEB128($writer, $minDelta);

        $relativeDeltas = $this->deltaCalculator->calculateRelativeDeltas($blockDeltas, $minDelta);
        $miniblockCount = (int) ceil(count($relativeDeltas) / $this->miniblockSize);

        $bitWidths = [];
        $packedMiniblocks = [];

        for ($miniblockIndex = 0; $miniblockIndex < $miniblockCount; $miniblockIndex++) {
            $miniblockStart = $miniblockIndex * $this->miniblockSize;
            $miniblockEnd = min($miniblockStart + $this->miniblockSize, count($relativeDeltas));
            $miniblockDeltas = array_slice($relativeDeltas, $miniblockStart, $miniblockEnd - $miniblockStart);

            $padded = $this->padMiniblock($miniblockDeltas);
            $bitWidth = BitWidth::fromArray($padded);
            $bitWidths[] = $bitWidth;

            $packedMiniblocks[] = $this->packMiniblock($padded, $bitWidth);
        }

        // a partial last block still carries every miniblock's bit width, the unused ones zero, without their bodies
        $writer->writeBytes(array_pad($bitWidths, intdiv($this->blockSize, $this->miniblockSize), 0));

        foreach ($packedMiniblocks as $packed) {
            $writer->append($packed);
        }
    }

    /**
     * @param array<int> $values
     */
    private function writeBlocks(BinaryBufferWriter $writer, array $values): void
    {
        if (count($values) <= 1) {
            return;
        }

        $deltas = $this->deltaCalculator->calculateDeltas($values);

        // INT32 deltas wrap at 32 bits, as arrow-rs and parquet-java write them, so relative deltas need at most 32 bits
        if ($this->physicalType === PhysicalType::INT32) {
            $deltas = array_map($this->deltaCalculator->toInt32(...), $deltas);
        }
        $blockCount = (int) ceil(count($deltas) / $this->blockSize);

        for ($blockIndex = 0; $blockIndex < $blockCount; $blockIndex++) {
            $blockStart = $blockIndex * $this->blockSize;
            $blockEnd = min($blockStart + $this->blockSize, count($deltas));
            $blockDeltas = array_slice($deltas, $blockStart, $blockEnd - $blockStart);

            $this->writeBlock($writer, $blockDeltas);
        }
    }

    /**
     * @param array<int> $values
     */
    private function writeHeader(BinaryBufferWriter $writer, array $values): void
    {
        $miniblockCount = (int) ($this->blockSize / $this->miniblockSize);

        $this->writeULEB128($writer, $this->blockSize);
        $this->writeULEB128($writer, $miniblockCount);
        $this->writeULEB128($writer, count($values));
        $this->writeSignedLEB128($writer, $values[0] ?? 0);
    }

    private function writeSignedLEB128(BinaryBufferWriter $writer, int $value): void
    {
        $writer->writeVarInts([$this->zigzag->encode($value)]);
    }

    private function writeULEB128(BinaryBufferWriter $writer, int $value): void
    {
        $writer->writeVarInts([$value]);
    }
}
