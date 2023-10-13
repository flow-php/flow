<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\DataSize;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class RLEBitPackedHybrid
{
    public function __construct(private readonly LoggerInterface $logger = new NullLogger())
    {
    }

    /**
     * @psalm-suppress MixedOperand
     * @psalm-suppress MixedAssignment
     *
     * $output is passed by reference as a performance optimization, otherwise we would need to return the array and merge
     * it, which creates unnecessary performance impact.
     */
    public function decodeBitPacked(BinaryReader $reader, int $bitWidth, int $varInt, int $maxItems, array &$output) : void
    {
        $numGroups = $varInt >> 1;

        if ($numGroups === 0) {
            return;
        }

        $count = $numGroups * 8;
        $totalByteCount = (int) (($bitWidth * $count) / 8);
        $remainingByteCount = $reader->remainingLength()->bytes();
        $readBytes = $reader->readBytes(\min($remainingByteCount, $totalByteCount));
        $actualByteCount = $readBytes->count();

        $bitMask = (1 << $bitWidth) - 1;
        $byteIndex = 0;
        $currentByte = $readBytes[$byteIndex];
        $totalBits = $actualByteCount * 8;
        $bitsLeftInByte = 8;
        $bitsReadFromByte = 0;

        $resultIndex = 0;

        while ($totalBits >= $bitWidth && $resultIndex < $maxItems) {
            if ($bitsReadFromByte >= 8) {
                $bitsReadFromByte -= 8;
                $bitsLeftInByte -= 8;
                $currentByte >>= 8;
            } elseif ($bitsLeftInByte - $bitsReadFromByte >= $bitWidth) {
                $decodedValue = (($currentByte >> $bitsReadFromByte) & $bitMask);
                $totalBits -= $bitWidth;
                $bitsReadFromByte += $bitWidth;
                $resultIndex++;
                $output[] = $decodedValue;
            } elseif ($byteIndex + 1 < $actualByteCount) {
                $byteIndex += 1;
                $currentByte |= ($readBytes[$byteIndex] << $bitsLeftInByte);
                $bitsLeftInByte += 8;
            }
        }
    }

    public function decodeHybrid(BinaryReader $reader, int $bitWidth, int $maxItems, DataSize $length = null) : array
    {
        $length = ($length) ?: new DataSize($reader->readInt32() * 8);

        $output = [];
        $start = $reader->position();

        $iteration = 0;

        while (($reader->position()->bytes() - $start->bytes()) < $length->bytes() && \count($output) < $maxItems) {
            $iteration++;
            $varInt = $reader->readVarInt();
            $isRle = ($varInt & 1) === 0;

            $this->debugLog($iteration, $varInt, $isRle, $bitWidth, $length, $reader, $output);

            if ($isRle) {
                $this->decodeRLE($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            } else {
                $this->decodeBitPacked($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            }
        }

        return \array_slice($output, 0, $maxItems);
    }

    public function decodeRLE(BinaryReader $reader, int $bitWidth, int $intVar, int $maxItems, array &$output) : void
    {
        $isLiteralRun = $intVar & 1;
        $runLength = $intVar >> 1;

        if ($runLength === 0) {
            return;
        }

        $count = \min($runLength, $maxItems);
        $width = (int) (($bitWidth + 7) / 8);
        $value = $width > 0 ? $reader->readBytes($width)->toInt() : 0;

        if ($isLiteralRun) {
            for ($i = 0; $i < $count; $i++) {
                $output[] = $reader->readBits($bitWidth);
            }
        } else {
            for ($i = 0; $i < $count; $i++) {
                $output[] = $value;
            }
        }
    }

    private function debugLog(int $iteration, int $varInt, bool $isRle, int $bitWidth, DataSize $length, BinaryReader $reader, array $output) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('RLE/BytePacked hybrid decoding', [
            'iteration' => $iteration,
            'var_int' => $varInt,
            'is_rle' => $isRle,
            'bit_width' => $bitWidth,
            'length' => ['bits' => $length->bits(), 'bytes' => $length->bytes()],
            'reader_position' => ['bits' => $reader->position()->bits(), 'bytes' => $reader->position()->bytes()],
            'output_count' => \count($output),
        ]);
    }
}
