<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class DeltaBinaryPackedEncoder
{
    private const DEFAULT_BLOCK_SIZE = 128;

    private const DEFAULT_MINIBLOCK_SIZE = 32;

    public function __construct(
        private int $blockSize = self::DEFAULT_BLOCK_SIZE,
        private int $miniblockSize = self::DEFAULT_MINIBLOCK_SIZE,
        private DeltaCalculator $deltaCalculator = new DeltaCalculator(),
        private ZigZag $zigzag = new ZigZag(),
    ) {
        if ($this->blockSize % 128 !== 0) {
            throw new InvalidArgumentException('Block size must be a multiple of 128');
        }

        if ($this->miniblockSize % 32 !== 0) {
            throw new InvalidArgumentException('Miniblock size must be a multiple of 32');
        }

        if ($this->blockSize % $this->miniblockSize !== 0) {
            throw new InvalidArgumentException('Block size must be a multiple of miniblock size');
        }
    }

    /**
     * @param array<int> $values
     */
    public function encode(array $values) : string
    {
        if (!\count($values)) {
            return '';
        }

        foreach ($values as $index => $value) {
            if (!is_int($value)) {
                throw new InvalidArgumentException('Delta encoding requires integer values, got ' . gettype($value) . " at index {$index}: " . var_export($value, true));
            }
        }

        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        $this->writeHeader($writer, $values);
        $this->writeBlocks($writer, $values);

        return $buffer;
    }

    /**
     * @param array<int> $values
     */
    private function packMiniblock(array $values, int $bitWidth) : string
    {
        if ($bitWidth === 0) {
            return str_repeat("\x00", (int) ceil($this->miniblockSize / 8));
        }

        $buffer = '';
        $writer = new BinaryBufferWriter($buffer);

        // For large bit widths (>= 62), use bit-by-bit packing to avoid overflow
        if ($bitWidth >= 62) {
            return $this->packMiniblockSafe($values, $bitWidth, $writer);
        }

        // Use the original fast method for smaller bit widths
        $currentByte = 0;
        $bitsInByte = 0;

        foreach ($values as $value) {
            $currentByte |= ($value << $bitsInByte);
            $bitsInByte += $bitWidth;

            while ($bitsInByte >= 8) {
                $writer->writeBytes([$currentByte & 0xFF]);
                $currentByte >>= 8;
                $bitsInByte -= 8;
            }
        }

        if ($bitsInByte > 0) {
            $writer->writeBytes([$currentByte & 0xFF]);
        }

        $expectedByteCount = (int) ceil(($this->miniblockSize * $bitWidth) / 8);

        while (strlen($buffer) < $expectedByteCount) {
            $writer->writeBytes([0]);
        }

        return $buffer;
    }

    /**
     * Safe bit packing for large bit widths to avoid integer overflow.
     * Uses manual bit manipulation with array of bytes instead of integer accumulation.
     *
     * @param array<int> $values
     */
    private function packMiniblockSafe(array $values, int $bitWidth, BinaryBufferWriter $writer) : string
    {
        $expectedByteCount = (int) ceil(($this->miniblockSize * $bitWidth) / 8);
        $bytes = array_fill(0, $expectedByteCount, 0);

        $globalBitOffset = 0;

        foreach ($values as $value) {
            // Pack this value bit by bit
            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $bitValue = ($value >> $bit) & 1;

                if ($bitValue) {
                    $byteIndex = intdiv($globalBitOffset, 8);
                    $bitIndex = $globalBitOffset % 8;

                    if ($byteIndex < $expectedByteCount) {
                        $bytes[$byteIndex] |= (1 << $bitIndex);
                    }
                }

                $globalBitOffset++;
            }
        }

        // Return the packed data as a string (don't write to the passed writer)
        return pack('C*', ...$bytes);
    }

    /**
     * @param array<int> $miniblockDeltas
     *
     * @return array<int>
     */
    private function padMiniblock(array $miniblockDeltas) : array
    {
        $padded = $miniblockDeltas;

        while (\count($padded) < $this->miniblockSize) {
            $padded[] = 0;
        }

        return $padded;
    }

    /**
     * @param array<int> $blockDeltas
     */
    private function writeBlock(BinaryBufferWriter $writer, array $blockDeltas) : void
    {
        if (!\count($blockDeltas)) {
            return;
        }

        $minDelta = min($blockDeltas);
        $this->writeSignedLEB128($writer, $minDelta);

        $relativeDeltas = array_map(fn ($delta) => $this->deltaCalculator->calculateRelativeDelta($delta, $minDelta), $blockDeltas);
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

        $writer->writeBytes($bitWidths);

        foreach ($packedMiniblocks as $packed) {
            $writer->append($packed);
        }
    }

    /**
     * @param array<int> $values
     */
    private function writeBlocks(BinaryBufferWriter $writer, array $values) : void
    {
        if (count($values) <= 1) {
            return;
        }

        $deltas = $this->deltaCalculator->calculateDeltas($values);
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
    private function writeHeader(BinaryBufferWriter $writer, array $values) : void
    {
        $miniblockCount = $this->blockSize / $this->miniblockSize;

        $this->writeULEB128($writer, $this->blockSize);
        $this->writeULEB128($writer, $miniblockCount);
        $this->writeULEB128($writer, count($values));
        $this->writeSignedLEB128($writer, $values[0]);
    }

    private function writeSignedLEB128(BinaryBufferWriter $writer, int $value) : void
    {
        $writer->writeVarInts([$this->zigzag->encode($value)]);
    }

    private function writeULEB128(BinaryBufferWriter $writer, int $value) : void
    {
        $writer->writeVarInts([$value]);
    }
}
