<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\DataSize;
use Flow\Parquet\Exception\OutOfBoundsException;
use Generator;

use function intdiv;
use function ord;
use function strlen;
use function substr;

final class BinaryBufferReader implements BinaryReader
{
    private int $lengthBits;

    private int $positionBits;

    private int $positionBytes;

    private int $remainingBytes;

    public function __construct(
        private readonly string $buffer,
    ) {
        $this->positionBits = 0;
        $this->positionBytes = 0;
        $bufferLen = strlen($buffer);
        $this->remainingBytes = $bufferLen;
        $this->lengthBits = $bufferLen * 8;
    }

    public function length(): DataSize
    {
        return new DataSize($this->lengthBits);
    }

    public function position(): DataSize
    {
        return new DataSize($this->positionBits);
    }

    public function readBits(int $total): Generator
    {
        $bytePosition = $this->positionBytes;
        $bitOffset = $this->positionBits % 8;
        $bytesNeeded = intdiv($bitOffset + $total - 1, 8) + 1;
        $currentBytes = substr($this->buffer, $bytePosition, $bytesNeeded);
        $bitsRead = 0;

        for ($i = 0; $i < $bytesNeeded; $i++) {
            $byte = ord($currentBytes[$i] ?? '');

            for ($j = $bitOffset; $j < 8; $j++) {
                yield ($byte >> $j) & 1;
                $bitsRead++;

                if ($bitsRead === $total) {
                    $bitsAdvanced = ($i * 8) + $j + 1 - $bitOffset;
                    $this->positionBits += $bitsAdvanced;
                    $this->positionBytes = intdiv($this->positionBits, 8);
                    $this->remainingBytes = intdiv($this->lengthBits - $this->positionBits, 8);

                    return;
                }
            }
            $bitOffset = 0;
        }
    }

    public function readBytes(int $total): string
    {
        $raw = substr($this->buffer, $this->positionBytes, $total);

        $this->positionBits += 8 * $total;
        $this->positionBytes += $total;
        $this->remainingBytes -= $total;

        return $raw;
    }

    public function readVarInt(): int
    {
        $result = 0;
        $shift = 0;

        do {
            if ($this->positionBytes >= strlen($this->buffer)) {
                throw new OutOfBoundsException('Buffer overflow: attempted to read beyond buffer length in readVarInt');
            }

            $byte = ord($this->buffer[$this->positionBytes]);
            $this->positionBits += 8;
            $this->positionBytes++;
            $this->remainingBytes--;

            $result |= ($byte & 0x7F) << $shift;
            $shift += 7;
        } while ($byte >= 0x80);

        return $result;
    }

    public function remainingLength(): DataSize
    {
        return DataSize::fromBytes($this->remainingBytes);
    }

    public function seekBits(int $bits): void
    {
        $this->positionBits += $bits;
        $this->positionBytes = intdiv($this->positionBits, 8);
        $this->lengthBits -= $bits;
    }

    public function seekBytes(int $bytes): void
    {
        $this->positionBits += $bytes * 8;
        $this->positionBytes += $bytes;
        $this->remainingBytes -= $bytes;
    }
}
