<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\Binary\Bytes;
use Flow\Parquet\{BinaryReader, DataSize};

final readonly class BinaryBufferReader implements BinaryReader
{
    private DataSize $length;

    private DataSize $position;

    private DataSize $remainingLength;

    public function __construct(private string $buffer)
    {
        $this->position = new DataSize(0);
        $bits = \strlen($buffer) * 8;
        $this->remainingLength = new DataSize($bits);
        $this->length = new DataSize($bits);
    }

    public function length() : DataSize
    {
        return $this->length;
    }

    public function position() : DataSize
    {
        return $this->position;
    }

    public function readBits(int $total) : \Generator
    {
        $bytePosition = $this->position()->bytes();
        $bitOffset = $this->position->bits() % 8;
        $bytesNeeded = \intdiv($bitOffset + $total - 1, 8) + 1;
        $currentBytes = \substr($this->buffer, $bytePosition, $bytesNeeded);
        $bitsRead = 0;

        for ($i = 0; $i < $bytesNeeded; $i++) {
            $byte = \ord($currentBytes[$i] ?? '');

            for ($j = $bitOffset; $j < 8; $j++) {
                yield ($byte >> $j) & 1;
                $bitsRead++;

                if ($bitsRead === $total) {
                    $this->position->add($i * 8 + $j + 1 - $bitOffset);
                    $this->remainingLength->sub($i * 8 + $j + 1 - $bitOffset);

                    return;
                }
            }
            $bitOffset = 0;
        }
    }

    public function readBytes(int $total) : Bytes
    {
        $bytes = \array_values(\unpack('C*', \substr($this->buffer, $this->position()->bytes(), $total)));

        $this->position->add(8 * $total);
        $this->remainingLength->sub(8 * $total);

        return new Bytes($bytes);
    }

    public function readVarInt() : int
    {
        $result = 0;
        $shift = 0;

        do {
            $bytes = $this->readBytes(1);

            if ($bytes->count() === 0) {
                break;
            }

            $byte = $bytes->toArray()[0];
            $result |= ($byte & 0x7F) << $shift;
            $shift += 7;
        } while ($byte >= 0x80);

        return $result;
    }

    public function remainingLength() : DataSize
    {
        return $this->remainingLength;
    }

    public function seekBits(int $bits) : void
    {
        $this->position->add($bits);
        $this->length->sub($bits);
    }

    public function seekBytes(int $bytes) : void
    {
        $this->position->add($bytes * 8);
        $this->remainingLength->sub($bytes * 8);
    }
}
