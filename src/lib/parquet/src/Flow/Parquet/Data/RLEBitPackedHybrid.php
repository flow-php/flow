<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\BinaryWriter;

final class RLEBitPackedHybrid
{
    public function __construct() {}

    /**
     * $output is passed by reference as a performance optimization, otherwise we would need to return the array and merge
     * it, which creates unnecessary performance impact.
     *
     * @param array<int> $output
     */
    public function decodeBitPacked(
        BinaryReader $reader,
        int $bitWidth,
        int $varInt,
        int $maxItems,
        array &$output,
    ): void {
        $numGroups = $varInt >> 1;

        if ($numGroups === 0) {
            return;
        }

        if ($bitWidth === 0) {
            $output = \array_merge($output, \array_fill(0, \min($numGroups * 8, $maxItems), 0));

            return;
        }

        $count = $numGroups * 8;
        $totalByteCount = (int) (($bitWidth * $count) / 8);

        $remainingByteCount = $reader->remainingLength()->bytes();
        $readRaw = $reader->readBytes(\min($remainingByteCount, $totalByteCount));
        $actualByteCount = \strlen($readRaw);

        if ($actualByteCount === 0) {
            return;
        }

        $bitMask = (1 << $bitWidth) - 1;
        $byteIndex = 0;
        $currentByte = \ord($readRaw[0]);
        $totalBits = $actualByteCount * 8;
        $bitsLeftInByte = 8;
        $bitsReadFromByte = 0;

        $resultIndex = 0;

        while ($totalBits >= $bitWidth && $resultIndex < $maxItems) {
            if ($bitsReadFromByte >= 8) {
                $bitsReadFromByte -= 8;
                $bitsLeftInByte -= 8;
                $currentByte >>= 8;
            } elseif (($bitsLeftInByte - $bitsReadFromByte) >= $bitWidth) {
                $decodedValue = ($currentByte >> $bitsReadFromByte) & $bitMask;
                $totalBits -= $bitWidth;
                $bitsReadFromByte += $bitWidth;
                $resultIndex++;
                $output[] = $decodedValue;
            } elseif (($byteIndex + 1) < $actualByteCount) {
                $byteIndex++;
                $currentByte |= \ord($readRaw[$byteIndex]) << $bitsLeftInByte;
                $bitsLeftInByte += 8;
            }
        }
    }

    /**
     * @return array<int>
     */
    public function decodeHybrid(BinaryReader $reader, int $bitWidth, int $maxItems): array
    {
        /** @var array<int> $output */
        $output = [];

        while (\count($output) < $maxItems) {
            $varInt = $reader->readVarInt();
            $isRle = ($varInt & 1) === 0;

            if ($isRle) {
                $this->decodeRLE($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            } else {
                $this->decodeBitPacked($reader, $bitWidth, $varInt, $maxItems - \count($output), $output);
            }
        }

        return $output;
    }

    /**
     * @param array<int> $output
     */
    public function decodeRLE(BinaryReader $reader, int $bitWidth, int $intVar, int $maxItems, array &$output): void
    {
        $isLiteralRun = $intVar & 1;
        $runLength = $intVar >> 1;

        if ($runLength === 0) {
            $output[] = 0;

            return;
        }

        $count = \min($runLength, $maxItems);
        $width = (int) (($bitWidth + 7) / 8);

        if ($width > 0) {
            $raw = $reader->readBytes($width);
            $value = 0;

            for ($i = 0; $i < $width; $i++) {
                $value |= \ord($raw[$i]) << ($i * 8);
            }
        } else {
            $value = 0;
        }

        if ($isLiteralRun) {
            for ($i = 0; $i < $count; $i++) {
                /** @phpstan-ignore-next-line */
                $output[] = \iterator_to_array($reader->readBits($bitWidth));
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
    public function encodeBitPacked(BinaryWriter $writer, int $bitWidth, array $values): void
    {
        $count = \count($values);
        $numGroups = ($count + 7) >> 3;
        $varInt = ($numGroups << 1) | 1;

        $packed = '';

        while ($varInt >= 0x80) {
            $packed .= \chr(($varInt & 0x7F) | 0x80);
            $varInt >>= 7;
        }
        $packed .= \chr($varInt & 0x7F);

        $buffer = 0;
        $bitsInBuffer = 0;
        /** @var array<int> $bytes */
        $bytes = [];

        foreach ($values as $value) {
            $buffer |= $value << $bitsInBuffer;
            $bitsInBuffer += $bitWidth;

            while ($bitsInBuffer >= 8) {
                $bytes[] = $buffer & 0xFF;
                $buffer >>= 8;
                $bitsInBuffer -= 8;
            }
        }

        if ($bitsInBuffer > 0) {
            $bytes[] = $buffer & 0xFF;
        }

        $expectedBytesCount = (int) (($numGroups * 8 * $bitWidth) / 8);
        $byteCount = \count($bytes);

        if ($byteCount < $expectedBytesCount) {
            \array_push($bytes, ...\array_fill(0, $expectedBytesCount - $byteCount, 0));
        }

        $writer->append($packed . \pack('C*', ...$bytes));
    }

    /**
     * @param array<int> $values
     */
    public function encodeHybrid(BinaryWriter $writer, int $bitWidth, array $values): void
    {
        $rleBuffer = [];
        $bitPackedBuffer = [];

        $previousValue = null;

        foreach ($values as $value) {
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

            if (\count($bitPackedBuffer) && (\count($bitPackedBuffer) % 8) === 0) {
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

                \array_push($bitPackedBuffer, ...$rleBuffer);
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
                \array_push($bitPackedBuffer, ...$rleBuffer);
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
    public function encodeRLE(BinaryWriter $writer, int $bitWidth, array $values): void
    {
        $repeatCount = \count($values);
        $intVar = $repeatCount << 1;

        $value = $values[0];

        $packed = '';

        while ($intVar >= 0x80) {
            $packed .= \chr(($intVar & 0x7F) | 0x80);
            $intVar >>= 7;
        }
        $packed .= \chr($intVar & 0x7F);

        $width = (int) (($bitWidth + 7) / 8);

        for ($i = 0; $i < $width; $i++) {
            $packed .= \chr(($value >> ($i * 8)) & 0xFF);
        }

        $writer->append($packed);
    }
}
