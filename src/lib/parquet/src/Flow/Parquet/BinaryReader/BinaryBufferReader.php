<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\{BinaryReader, ByteOrder, DataSize};

final readonly class BinaryBufferReader implements BinaryReader
{
    private DataSize $length;

    private DataSize $position;

    private DataSize $remainingLength;

    public function __construct(private string $buffer, private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN)
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

    public function readBooleans(int $total) : \Generator
    {
        foreach ($this->readBits($total) as $bit) {
            yield (bool) $bit;
        }
    }

    public function readByteArrays(int $total) : \Generator
    {
        $position = $this->position()->bytes();
        $count = 0;

        while ($count < $total) {
            $rawStr = \substr($this->buffer, $position, 4);

            if ($rawStr === '') {
                break;
            }
            // Read the length of the string from the first byte
            $bytesLength = \unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'V' : 'N', $rawStr)[1];
            $position += 4;

            $byteStr = \substr($this->buffer, $position, $bytesLength);

            $byteArray = \unpack('C*', $byteStr);

            yield new Bytes($byteArray, $this->byteOrder);
            $position += $bytesLength;
            $count++;
        }

        $this->position->add($position * 8);
        $this->remainingLength->sub($position * 8);
    }

    public function readBytes(int $total) : Bytes
    {
        $bytes = \array_values(\unpack('C*', \substr($this->buffer, $this->position()->bytes(), $total)));

        $this->position->add(8 * $total);
        $this->remainingLength->sub(8 * $total);

        return new Bytes($bytes);
    }

    public function readDecimals(int $total, int $byteLength, int $precision = 10, int $scale = 2) : \Generator
    {
        $allBytes = $this->readBytes($byteLength * $total)->toArray();
        $decimalBytes = \array_chunk($allBytes, $byteLength);

        $divisor = \bcpow('10', (string) $scale);

        foreach ($decimalBytes as $bytes) {
            $intValue = 0;

            foreach ($bytes as $i => $byte) {
                $shift = ($byteLength - 1 - $i) * 8;
                $intValue |= ($byte << $shift);
            }

            yield (float) \bcdiv((string) $intValue, $divisor, $scale);
        }
    }

    public function readDoubles(int $total) : \Generator
    {
        $doubleBytes = \array_chunk($this->readBytes(8 * $total)->toArray(), 8);

        foreach ($doubleBytes as $bytes) {
            yield \unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'e' : 'E', \pack('C*', ...$bytes))[1];
        }
    }

    public function readFloats(int $total) : \Generator
    {
        $floatBytes = \array_chunk($this->readBytes(4 * $total)->toArray(), 4);

        foreach ($floatBytes as $bytes) {
            yield \round(\unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'g' : 'G', \pack('C*', ...$bytes))[1], 7);
        }
    }

    public function readInts16(int $total) : \Generator
    {
        $intBytes = \array_chunk($this->readBytes(2 * $total)->toArray(), 2);

        foreach ($intBytes as $bytes) {

            if ($this->byteOrder === ByteOrder::LITTLE_ENDIAN) {
                $integer = $bytes[0] | ($bytes[1] << 8);
            } else {
                $integer = ($bytes[0] << 24) | ($bytes[1] << 16);
            }

            if ($integer & 0x8000) {
                $integer = -((~$integer & 0xFFFF) + 1);
            }

            yield $integer;
        }
    }

    public function readInts32(int $total) : \Generator
    {
        $intBytes = \array_chunk($this->readBytes(4 * $total)->toArray(), 4);

        foreach ($intBytes as $bytes) {
            if ($this->byteOrder === ByteOrder::LITTLE_ENDIAN) {
                $int = $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24);
            } else {
                $int = ($bytes[0] << 24) | ($bytes[1] << 16) | ($bytes[2] << 8) | $bytes[3];
            }

            if ($int & 0x80000000) {
                $int = -((~$int & 0xFFFFFFFF) + 1);  // Two's complement
            }

            yield $int;
        }
    }

    public function readInts64(int $total) : \Generator
    {
        $intBytes = \array_chunk($this->readBytes(8 * $total)->toArray(), 8);

        foreach ($intBytes as $bytes) {
            if ($this->byteOrder === ByteOrder::LITTLE_ENDIAN) {
                $int = $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24) |
                    ($bytes[4] << 32) | ($bytes[5] << 40) | ($bytes[6] << 48) | ($bytes[7] << 56);
                $sign = $bytes[7];
            } else {
                $int = ($bytes[0] << 56) | ($bytes[1] << 48) | ($bytes[2] << 40) | ($bytes[3] << 32) |
                    ($bytes[4] << 24) | ($bytes[5] << 16) | ($bytes[6] << 8) | $bytes[7];
                $sign = $bytes[7];
            }

            if ($sign & 0x80) {
                $int |= (-1 ^ 0xFFFFFFFFFFFFFFFF) << 56;
            } else {
                $int |= $sign << 56;
            }

            yield $int;
        }
    }

    public function readInts96(int $total) : \Generator
    {
        $intsData = \substr($this->buffer, $this->position()->bytes(), 12 * $total);

        foreach (\str_split($intsData, 12) as $data) {
            $int96Bytes = [];

            foreach (\str_split($data) as $byte) {
                $int96Bytes[] = \ord($byte);
            }

            yield new Bytes($int96Bytes, $this->byteOrder);
        }

        $this->position->add(12 * $total * 8);
        $this->remainingLength->sub(12 * $total * 8);
    }

    public function readStrings(int $total) : \Generator
    {
        $position = $this->position()->bytes();
        $count = 0;

        while ($count < $total) {
            $rawStr = \substr($this->buffer, $position, 4);

            if ($rawStr === '') {
                break;
            }
            // Read the length of the string from the first byte
            $strLength = \unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'V' : 'N', $rawStr)[1];
            $position += 4;

            // Read the string based on the length
            yield \substr($this->buffer, $position, $strLength);
            $position += $strLength;
            $count++;
        }

        $this->position->add($position * 8);
        $this->remainingLength->sub($position * 8);
    }

    public function readUInts32(int $total) : \Generator
    {
        $intBytes = \array_chunk($this->readBytes(4 * $total)->toArray(), 4);

        foreach ($intBytes as $bytes) {
            if ($this->byteOrder === ByteOrder::LITTLE_ENDIAN) {
                yield $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24);
            } else {
                yield ($bytes[0] << 24) | ($bytes[1] << 16) | ($bytes[2] << 8) | $bytes[3];
            }
        }
    }

    public function readUInts64(int $total) : \Generator
    {
        $intBytes = \array_chunk($this->readBytes(8 * $total)->toArray(), 8);

        foreach ($intBytes as $bytes) {
            if ($this->byteOrder === ByteOrder::LITTLE_ENDIAN) {
                yield $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24) |
                    ($bytes[4] << 32) | ($bytes[5] << 40) | ($bytes[6] << 48) | ($bytes[7] << 56);
            } else {
                yield ($bytes[0] << 56) | ($bytes[1] << 48) | ($bytes[2] << 40) | ($bytes[3] << 32) |
                    ($bytes[4] << 24) | ($bytes[5] << 16) | ($bytes[6] << 8) | $bytes[7];
            }
        }
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
