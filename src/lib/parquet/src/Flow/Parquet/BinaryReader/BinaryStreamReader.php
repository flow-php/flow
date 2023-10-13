<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryReader;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\DataSize;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\OutOfBoundsException;
use Flow\Parquet\Exception\RuntimeException;

final class BinaryStreamReader implements BinaryReader
{
    private int $bitPosition;

    private int $fileLength;

    /**
     * @param resource $handle
     */
    public function __construct(private $handle, private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN)
    {
        if (!\is_resource($handle)) {
            throw new InvalidArgumentException('Invalid file handle.');
        }

        $streamMetadata = \stream_get_meta_data($handle);

        if (!$streamMetadata['seekable']) {
            throw new InvalidArgumentException('File is not seekable');
        }

        \fseek($this->handle, 0, SEEK_END);
        $this->fileLength = \ftell($this->handle) ?: 0;
        \fseek($this->handle, 0, SEEK_SET);

        $this->bitPosition = 0;
    }

    public function length() : DataSize
    {
        return new DataSize($this->fileLength * 8);
    }

    public function position() : DataSize
    {
        return new DataSize($this->bitPosition);
    }

    public function readBit() : int
    {
        if ($this->bitPosition >= $this->length()->bits()) {
            throw new OutOfBoundsException('Reached the end of the file');
        }

        \fseek($this->handle, \intdiv($this->bitPosition, 8));
        $byte = \ord(\fread($this->handle, 1) ?: '');
        $bit = ($byte >> ($this->bitPosition % 8)) & 1;

        $this->bitPosition++;

        return $bit;
    }

    public function readBits(int $total) : array
    {
        if ($total < 0) {
            throw new InvalidArgumentException('Count cannot be negative.');
        }

        $bits = [];
        $bytePosition = \intdiv($this->bitPosition, 8);
        \fseek($this->handle, $bytePosition);

        while ($total > 0) {
            $byte = \ord(\fread($this->handle, 1) ?: '');

            for ($bitOffset = $this->bitPosition % 8; $bitOffset < 8; $bitOffset++) {
                $bits[] = ($byte >> $bitOffset) & 1;
                $total--;
                $this->bitPosition++;

                if ($total === 0) {
                    return $bits;
                }
            }
        }

        return $bits;
    }

    public function readBoolean() : bool
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readBooleans(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readByte() : int
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read a full byte.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $byte = \ord(\fread($this->handle, 1) ?: '');
        $this->bitPosition += 8;

        return $byte;
    }

    public function readByteArrays(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readBytes(int $total) : Bytes
    {
        if ($total < 0) {
            throw new InvalidArgumentException('Count cannot be negative.');
        }

        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read bytes.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \fread($this->handle, $total) ?: '';
        $this->bitPosition += 8 * \strlen($bytes);

        return new Bytes(\array_values(\unpack('C*', $bytes) ?: []));
    }

    public function readDouble() : float
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read a double.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \fread($this->handle, 8);
        $result = \unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'e' : 'E', $bytes)[1];
        $this->bitPosition += 64;

        return $result;
    }

    public function readDoubles(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readFloat() : float
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read a float.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \fread($this->handle, 4);
        $result = \unpack($this->byteOrder === ByteOrder::LITTLE_ENDIAN ? 'g' : 'G', $bytes)[1];
        $this->bitPosition += 32;

        return $result;
    }

    public function readFloats(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readInt32() : int
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read an int32.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \array_values(\unpack('C*', \fread($this->handle, 4)));
        $this->bitPosition += 32;

        return $this->byteOrder === ByteOrder::LITTLE_ENDIAN
            ? $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24)
            : ($bytes[0] << 24) | ($bytes[1] << 16) | ($bytes[2] << 8) | $bytes[3];
    }

    public function readInt64() : int
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read an int64.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \array_values(\unpack('C*', \fread($this->handle, 8) ?: '') ?: []);
        $this->bitPosition += 64;

        return $this->byteOrder === ByteOrder::LITTLE_ENDIAN
            ? $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24)
            | ($bytes[4] << 32) | ($bytes[5] << 40) | ($bytes[6] << 48) | ($bytes[7] << 56)
            : ($bytes[0] << 56) | ($bytes[1] << 48) | ($bytes[2] << 40) | ($bytes[3] << 32)
            | ($bytes[4] << 24) | ($bytes[5] << 16) | ($bytes[6] << 8) | $bytes[7];
    }

    public function readInt96() : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readInts32(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readInts64(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readInts96(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readString() : string
    {
        // Read the string bytes
        return $this->readBytes($this->readInt32())->toString();
    }

    public function readStrings(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readUInt32() : int
    {
        if ($this->bitPosition % 8 !== 0) {
            throw new InvalidArgumentException('Current position must be at byte boundary to read an uint32.');
        }

        \fseek($this->handle, $this->bitPosition / 8);
        $bytes = \array_values(\unpack('C*', \fread($this->handle, 4) ?: '') ?: []);
        $this->bitPosition += 32;

        return $this->byteOrder === ByteOrder::LITTLE_ENDIAN
            ? $bytes[0] | ($bytes[1] << 8) | ($bytes[2] << 16) | ($bytes[3] << 24)
            : ($bytes[0] << 24) | ($bytes[1] << 16) | ($bytes[2] << 8) | $bytes[3];
    }

    public function readUInt64() : int
    {
        return $this->readInt64();
    }

    public function readUInts32(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readUInts64(int $total) : array
    {
        throw new RuntimeException('Not implemented yet.');
    }

    public function readVarInt() : int
    {
        $result = 0;
        $shift = 0;

        do {
            if ($this->bitPosition % 8 !== 0) {
                throw new InvalidArgumentException('Current position must be at byte boundary to read a varint.');
            }

            \fseek($this->handle, $this->bitPosition / 8);
            $byte = \ord(\fread($this->handle, 1) ?: '');
            $this->bitPosition += 8;

            $result |= ($byte & 0x7F) << $shift;
            $shift += 7;
        } while ($byte >= 0x80);

        return $result;
    }

    public function remainingLength() : DataSize
    {
        return new DataSize($this->length()->bits() - $this->bitPosition);
    }

    public function seekBits(int $bits) : void
    {
        $this->bitPosition += $bits;
        \fseek($this->handle, \intdiv($this->bitPosition, 8));
    }

    public function seekBytes(int $bytes) : void
    {
        $this->bitPosition += $bytes * 8;
        \fseek($this->handle, \intdiv($this->bitPosition, 8));
    }
}
