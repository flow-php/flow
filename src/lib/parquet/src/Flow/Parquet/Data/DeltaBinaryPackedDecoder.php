<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryReader\BinaryBufferReader;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;

final readonly class DeltaBinaryPackedDecoder
{
    private const int DEFAULT_BLOCK_SIZE = 128;

    private const int DEFAULT_MINIBLOCK_SIZE = 32;

    public function __construct(
        private int $blockSize = self::DEFAULT_BLOCK_SIZE,
        private int $miniblockSize = self::DEFAULT_MINIBLOCK_SIZE,
        private DeltaCalculator $deltaCalculator = new DeltaCalculator(),
        private ZigZag $zigzag = new ZigZag(),
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
     * @return array<int>
     */
    public function decode(string $data, int $valueCount): array
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
            $this->readBlocks($reader, $valueCount - 1, $header->blockSize),
        );
    }

    /**
     * @return array<int>
     */
    private function readBlock(BinaryBufferReader $reader, int $blockDeltaCount): array
    {
        $minDelta = $this->readSignedLEB128($reader);

        $miniblockCount = (int) ceil($blockDeltaCount / $this->miniblockSize);

        $bitWidths = [];

        for ($i = 0; $i < $miniblockCount; $i++) {
            $bitWidths[] = \ord($reader->readBytes(1));
        }

        $deltas = [];
        $deltasRead = 0;

        for (
            $miniblockIndex = 0;
            $miniblockIndex < $miniblockCount && $deltasRead < $blockDeltaCount;
            $miniblockIndex++
        ) {
            $bitWidth = $bitWidths[$miniblockIndex] ?? null;

            if ($bitWidth === null) {
                throw new \Flow\Parquet\Exception\RuntimeException('Missing bit width for miniblock index '
                . $miniblockIndex);
            }
            $miniblockSize = $this->miniblockSize;

            $remainingDeltas = $blockDeltaCount - $deltasRead;
            $valuesToRead = min($miniblockSize, $remainingDeltas);

            if ($bitWidth === 0) {
                $miniblockDeltas = array_fill(0, max(0, $valuesToRead), 0);
            } else {
                $packedSize = (int) ceil(($miniblockSize * $bitWidth) / 8);
                $packedRaw = $reader->readBytes($packedSize);
                $miniblockDeltas = $this->unpackMiniblockFromString($packedRaw, $bitWidth, $valuesToRead);
            }

            $actualDeltas = array_map(
                static function ($delta) use ($minDelta) {
                    // PHP converts int overflow to float at runtime, so $result may be float even though both operands are int.
                    // PHPStan models int arithmetic as never overflowing, which is why the is_float check below is suppressed.
                    $result = $delta + $minDelta;

                    // Handle float overflow precisely using BCMath
                    // @mago-ignore analysis:impossible-condition
                    // @phpstan-ignore-next-line
                    if (\is_float($result)) {
                        // Use BCMath for precise integer arithmetic
                        $preciseResult = \bcadd((string) $delta, (string) $minDelta, 0);

                        // Apply 2's complement wrapping for 64-bit integers
                        // @mago-ignore analysis:redundant-condition
                        // @mago-ignore analysis:redundant-comparison
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
                },
                $miniblockDeltas,
            );
            $deltas = array_merge($deltas, $actualDeltas);
            $deltasRead += count($actualDeltas);
        }

        return array_slice($deltas, 0, $blockDeltaCount);
    }

    /**
     * @return array<int>
     */
    private function readBlocks(BinaryBufferReader $reader, int $deltaCount, int $blockSize): array
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

    private function readHeader(BinaryBufferReader $reader): DeltaHeader
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

    private function readSignedLEB128(BinaryBufferReader $reader): int
    {
        return $this->zigzag->decode($reader->readVarInt());
    }

    private function readULEB128(BinaryBufferReader $reader): int
    {
        return $reader->readVarInt();
    }

    /**
     * @param array<int> $deltas
     *
     * @return array<int>
     */
    private function reconstructValues(int $firstValue, array $deltas): array
    {
        return $this->deltaCalculator->reconstructValues($firstValue, $deltas);
    }

    /**
     * @return array<int>
     */
    private function unpackMiniblockFromString(string $packedData, int $bitWidth, int $valuesToRead): array
    {
        if ($bitWidth >= 62) {
            return $this->unpackMiniblockFromStringSafe($packedData, $bitWidth, $valuesToRead);
        }

        $values = [];
        $bitOffset = 0;
        $dataLen = \strlen($packedData);

        for ($valueIndex = 0; $valueIndex < $valuesToRead; $valueIndex++) {
            $value = 0;

            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $byteIndex = intdiv($bitOffset, 8);
                $bitIndex = $bitOffset % 8;

                if ($byteIndex >= $dataLen) {
                    break;
                }

                $byte = \ord($packedData[$byteIndex]);
                $bitValue = ($byte >> $bitIndex) & 1;
                $value |= $bitValue << $bit;
                $bitOffset++;
            }

            $values[] = $value;
        }

        return $values;
    }

    /**
     * @return array<int>
     */
    private function unpackMiniblockFromStringSafe(string $packedData, int $bitWidth, int $valuesToRead): array
    {
        $values = [];
        $dataLen = \strlen($packedData);
        $globalBitOffset = 0;

        for ($valueIndex = 0; $valueIndex < $valuesToRead; $valueIndex++) {
            $value = 0;

            for ($bit = 0; $bit < $bitWidth; $bit++) {
                $byteIndex = intdiv($globalBitOffset, 8);
                $bitIndex = $globalBitOffset % 8;

                if ($byteIndex < $dataLen) {
                    $byte = \ord($packedData[$byteIndex]);
                    $bitValue = ($byte >> $bitIndex) & 1;

                    if ($bitValue) {
                        $value |= 1 << $bit;
                    }
                }

                $globalBitOffset++;
            }

            $values[] = $value;
        }

        return $values;
    }
}
