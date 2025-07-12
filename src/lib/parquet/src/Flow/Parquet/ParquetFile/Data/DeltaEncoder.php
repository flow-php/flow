<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Exception\InvalidArgumentException;

final readonly class DeltaEncoder
{
    private const DEFAULT_BLOCK_SIZE = 128;

    private const DEFAULT_MINIBLOCK_SIZE = 32;

    public function __construct(
        private int $blockSize = self::DEFAULT_BLOCK_SIZE,
        private int $miniblockSize = self::DEFAULT_MINIBLOCK_SIZE,
        private DeltaCalculator $deltaCalculator = new DeltaCalculator(),
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

        // Validate that all values are integers
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
     *
     * @return array<int>
     */
    private function calculateDeltas(array $values) : array
    {
        return $this->deltaCalculator->calculateDeltas($values);
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

        $relativeDeltas = array_map(fn ($delta) => (int) ($delta - $minDelta), $blockDeltas);
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

        $deltas = $this->calculateDeltas($values);
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
        $zigzag = $this->zigzagEncode($value);
        $this->writeULEB128($writer, $zigzag);
    }

    private function writeULEB128(BinaryBufferWriter $writer, int $value) : void
    {
        $bytes = [];

        while ($value >= 0x80) {
            $bytes[] = ($value & 0x7F) | 0x80;
            $value >>= 7;
        }
        $bytes[] = $value & 0x7F;
        $writer->writeBytes($bytes);
    }

    private function zigzagEncode(int $value) : int
    {
        if (PHP_INT_SIZE === 8) {
            return ($value << 1) ^ ($value >> 63);
        }

        return ($value << 1) ^ ($value >> 31);
    }
}
