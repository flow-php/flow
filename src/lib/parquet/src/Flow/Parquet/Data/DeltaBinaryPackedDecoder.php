<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\{InvalidArgumentException, RuntimeException};

final readonly class DeltaBinaryPackedDecoder
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
     * @return array<int>
     */
    public function decode(string $data, int $valueCount) : array
    {
        if ($valueCount === 0) {
            return [];
        }

        if ($data === '') {
            throw new RuntimeException('Cannot decode empty data when value count is greater than 0');
        }

        $header = $this->readHeader($reader = new BinaryBufferReader($data));

        if ($header->totalValues !== $valueCount) {
            throw new RuntimeException("Value count mismatch: expected {$valueCount}, got {$header->totalValues}");
        }

        if ($valueCount === 1) {
            return [$header->firstValue];
        }

        return $this->reconstructValues(
            $header->firstValue,
            $this->readBlocks($reader, $valueCount - 1, $header->blockSize)
        );
    }

    /**
     * @return array<int>
     */
    private function readBlock(BinaryBufferReader $reader, int $blockDeltaCount) : array
    {
        $minDelta = $this->readSignedLEB128($reader);

        $miniblockCount = (int) ceil($blockDeltaCount / $this->miniblockSize);

        $bitWidths = [];

        for ($i = 0; $i < $miniblockCount; $i++) {
            $bitWidths[] = $reader->readBytes(1)->toArray()[0];
        }

        $deltas = [];
        $deltasRead = 0;

        for ($miniblockIndex = 0; $miniblockIndex < $miniblockCount && $deltasRead < $blockDeltaCount; $miniblockIndex++) {
            $bitWidth = $bitWidths[$miniblockIndex];
            $miniblockSize = $this->miniblockSize;

            $remainingDeltas = $blockDeltaCount - $deltasRead;
            $valuesToRead = min($miniblockSize, $remainingDeltas);

            if ($bitWidth === 0) {
                $miniblockDeltas = array_fill(0, $valuesToRead, 0);
            } else {
                $packedSize = (int) ceil(($miniblockSize * $bitWidth) / 8);
                $packedData = $reader->readBytes($packedSize);
                $miniblockDeltas = $this->unpackMiniblock($packedData->toArray(), $bitWidth, $valuesToRead);
            }

            $actualDeltas = array_map(function ($delta) use ($minDelta) {
                $result = $delta + $minDelta;

                // Handle float overflow precisely using BCMath
                // @phpstan-ignore-next-line function.impossibleType - PHP can convert int overflow to float
                if (\is_float($result)) {
                    // Use BCMath for precise integer arithmetic
                    $preciseResult = \bcadd((string) $delta, (string) $minDelta, 0);

                    // Apply 2's complement wrapping for 64-bit integers
                    if (PHP_INT_SIZE === 8) {
                        if (\bccomp($preciseResult, (string) PHP_INT_MAX, 0) > 0) {
                            $preciseResult = \bcsub($preciseResult, '18446744073709551616', 0);
                        } elseif (\bccomp($preciseResult, (string) PHP_INT_MIN, 0) < 0) {
                            $preciseResult = \bcadd($preciseResult, '18446744073709551616', 0);
                        }
                    }

                    return (int) $preciseResult;
                }

                return $result;
            }, $miniblockDeltas);
            $deltas = array_merge($deltas, $actualDeltas);
            $deltasRead += count($actualDeltas);
        }

        return array_slice($deltas, 0, $blockDeltaCount);
    }

    /**
     * @return array<int>
     */
    private function readBlocks(BinaryBufferReader $reader, int $deltaCount, int $blockSize) : array
    {
        $deltas = [];
        $deltasRead = 0;

        while ($deltasRead < $deltaCount) {
            $remainingDeltas = $deltaCount - $deltasRead;
            $blockDeltaCount = min($blockSize, $remainingDeltas);

            $blockDeltas = $this->readBlock($reader, $blockDeltaCount);
            $deltas = array_merge($deltas, $blockDeltas);
            $deltasRead += count($blockDeltas);
        }

        return $deltas;
    }

    private function readHeader(BinaryBufferReader $reader) : DeltaHeader
    {
        $blockSize = $this->readULEB128($reader);
        $miniblockCount = $this->readULEB128($reader);
        $totalValues = $this->readULEB128($reader);
        $firstValue = $this->readSignedLEB128($reader);

        return new DeltaHeader(
            blockSize: $blockSize,
            miniblockCount: $miniblockCount,
            totalValues: $totalValues,
            firstValue: $firstValue,
        );
    }

    private function readSignedLEB128(BinaryBufferReader $reader) : int
    {
        return $this->zigzag->decode($reader->readVarInt());
    }

    private function readULEB128(BinaryBufferReader $reader) : int
    {
        return $reader->readVarInt();
    }

    /**
     * @param array<int> $deltas
     *
     * @return array<int>
     */
    private function reconstructValues(int $firstValue, array $deltas) : array
    {
        return $this->deltaCalculator->reconstructValues($firstValue, $deltas);
    }

    /**
     * @param array<int> $packedBytes
     *
     * @return array<int>
     */
    private function unpackMiniblock(array $packedBytes, int $bitWidth, int $valuesToRead) : array
    {
        // For large bit widths (>= 62), use safe unpacking to avoid overflow
        if ($bitWidth >= 62) {
            return $this->unpackMiniblockSafe($packedBytes, $bitWidth, $valuesToRead);
        }

        // Use the original fast method for smaller bit widths
        $values = [];
        $bitOffset = 0;
        $packedData = \pack('C*', ...$packedBytes);

        for ($valueIndex = 0; $valueIndex < $valuesToRead; $valueIndex++) {
            $value = 0;

            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $byteIndex = intdiv($bitOffset, 8);
                $bitIndex = $bitOffset % 8;

                if ($byteIndex >= strlen($packedData)) {
                    break;
                }

                $byte = ord($packedData[$byteIndex]);
                $bitValue = ($byte >> $bitIndex) & 1;
                $value |= ($bitValue << $bit);
                $bitOffset++;
            }

            $values[] = $value;
        }

        return $values;
    }

    /**
     * Safe bit unpacking for large bit widths to avoid integer overflow.
     * Uses the exact inverse of the safe packing algorithm.
     *
     * @param array<int> $packedBytes
     *
     * @return array<int>
     */
    private function unpackMiniblockSafe(array $packedBytes, int $bitWidth, int $valuesToRead) : array
    {
        $values = [];
        $packedData = \pack('C*', ...$packedBytes);

        $globalBitOffset = 0;

        for ($valueIndex = 0; $valueIndex < $valuesToRead; $valueIndex++) {
            $value = 0;

            // Unpack this value bit by bit
            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $byteIndex = intdiv($globalBitOffset, 8);
                $bitIndex = $globalBitOffset % 8;

                if ($byteIndex < strlen($packedData)) {
                    $byte = ord($packedData[$byteIndex]);
                    $bitValue = ($byte >> $bitIndex) & 1;

                    if ($bitValue) {
                        $value |= (1 << $bit);
                    }
                }

                $globalBitOffset++;
            }

            $values[] = $value;
        }

        return $values;
    }
}
