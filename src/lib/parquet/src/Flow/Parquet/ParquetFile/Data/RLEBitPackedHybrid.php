<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\BinaryWriter;
use Flow\Parquet\Exception\RuntimeException;
use Psr\Log\LoggerInterface;
use Psr\Log\NullLogger;

final class RLEBitPackedHybrid
{
    public function __construct(private readonly LoggerInterface $logger = new NullLogger())
    {
    }

    /**
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
                $byteIndex++;
                $currentByte |= ($readBytes[$byteIndex] << $bitsLeftInByte);
                $bitsLeftInByte += 8;
            }
        }
    }

    public function decodeHybrid(BinaryReader $reader, int $bitWidth, int $maxItems) : array
    {
        $output = [];

        $iteration = 0;

        while (\count($output) < $maxItems) {
            $iteration++;
            $varInt = $reader->readVarInt();
            $isRle = ($varInt & 1) === 0;

            $this->debugLog($iteration, $varInt, $isRle, $bitWidth, $reader, $output);

            if ($isRle) {
                $this->decodeRLE($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            } else {
                $this->decodeBitPacked($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            }
        }

        return $output;
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

    /**
     * @param array<int> $values
     */
    public function encodeBitPacked(BinaryWriter $writer, int $bitWidth, array $values) : void
    {
        $numGroups = (int) \ceil(\count($values) / 8.0);
        $varInt = ($numGroups << 1) | 1;

        $writer->writeVarInts32([$varInt]);

        $buffer = 0;
        $bitsInBuffer = 0;
        $bytes = [];

        foreach ($values as $value) {
            $buffer |= ($value << $bitsInBuffer);
            $bitsInBuffer += $bitWidth;

            while ($bitsInBuffer >= 8) {
                $bytes[] = $buffer & 0xFF;
                $buffer >>= 8;
                $bitsInBuffer -= 8;
            }
        }

        // Write any remaining bits in the buffer
        if ($bitsInBuffer > 0) {
            $bytes[] = $buffer & 0xFF;
        }

        $writer->writeBytes($bytes);
        $expectedBytesCount = (int) ((($numGroups * 8) * $bitWidth) / 8);

        while (\count($bytes) < $expectedBytesCount) {
            $writer->writeBytes([0]);
            $bytes[] = 0;
        }
    }

    /**
     * @param array<int> $values
     */
    public function encodeHybrid(BinaryWriter $writer, array $values) : void
    {
        $bitWidth = BitWidth::fromArray($values);

        $rleBuffer = [];
        $bitPackedBuffer = [];

        $previousValue = null;

        foreach ($values as $i => $value) {
            if ($previousValue === null) {
                $previousValue = $value;
                $rleBuffer[] = $value;

                continue;
            }

            // we always bit-pack a multiple of 8 values at a time, so we only store the number of "values / 8"
            if (\count($bitPackedBuffer) > 0 && \count($bitPackedBuffer) < 8) {
                $bitPackedBuffer[] = $value;

                continue;
            }

            if (\count($bitPackedBuffer) && \count($bitPackedBuffer) % 8 === 0) {
                $this->encodeBitPacked($writer, $bitWidth, $bitPackedBuffer);
                $bitPackedBuffer = [];
            }

            if ($previousValue === $value) {
                $rleBuffer[] = $value;
            } else {
                if (\count($rleBuffer) >= 8) {
                    if (\count($bitPackedBuffer)) {
                        $this->encodeBitPacked($writer, $bitWidth, $bitPackedBuffer);
                        $bitPackedBuffer = [];
                    }

                    $this->encodeRLE($writer, $bitWidth, $rleBuffer);
                    $rleBuffer = [];
                }

                $bitPackedBuffer = \array_merge($bitPackedBuffer, $rleBuffer);
                $bitPackedBuffer[] = $value;
                $rleBuffer = [];
            }

            $previousValue = $value;
        }

        if (\count($rleBuffer) > 8) {
            $this->encodeRLE($writer, $bitWidth, $rleBuffer);
            $rleBuffer = [];
        }

        if (\count($bitPackedBuffer)) {
            if (\count($rleBuffer)) {
                $bitPackedBuffer = \array_merge($bitPackedBuffer, $rleBuffer);
            }

            $this->encodeBitPacked($writer, $bitWidth, $bitPackedBuffer);
        }

        if (\count($rleBuffer)) {
            $bitPackedBuffer = $rleBuffer;
            $this->encodeBitPacked($writer, $bitWidth, $bitPackedBuffer);
        }
    }

    /**
     * @param array<int> $values
     */
    public function encodeRLE(BinaryWriter $writer, int $bitWidth, array $values) : void
    {
        if (\count(\array_unique($values)) !== 1) {
            throw new RuntimeException('RLE encoding only supports repeated values');
        }

        $repeatCount = \count($values);
        $intVar = ($repeatCount << 1);

        $value = $values[0];

        $writer->writeVarInts32([$intVar]);
        $writer->writeBytes(BitWidth::toBytes($value, $bitWidth));
    }

    private function debugLog(int $iteration, int $varInt, bool $isRle, int $bitWidth, BinaryReader $reader, array $output) : void
    {
        if ($this->logger instanceof NullLogger) {
            return;
        }

        $this->logger->debug('RLE/BytePacked hybrid decoding', [
            'iteration' => $iteration,
            'var_int' => $varInt,
            'is_rle' => $isRle,
            'bit_width' => $bitWidth,
            'reader_position' => ['bits' => $reader->position()->bits(), 'bytes' => $reader->position()->bytes()],
            'output_count' => \count($output),
        ]);
    }
}
