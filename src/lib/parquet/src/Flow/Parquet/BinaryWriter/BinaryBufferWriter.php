<?php declare(strict_types=1);

namespace Flow\Parquet\BinaryWriter;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\ByteOrder;
use Flow\Parquet\DataSize;

final class BinaryBufferWriter implements BinaryWriter
{
    public function __construct(private string &$buffer, private readonly ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN)
    {
        $this->buffer = '';
    }

    public function append(string $buffer) : void
    {
        $this->buffer .= $buffer;
    }

    public function length() : DataSize
    {
        return DataSize::fromBytes(\strlen($this->buffer));
    }

    public function writeBits(array $bits) : void
    {
        $byte = 0;
        $bitIndex = 0;

        foreach ($bits as $bit) {
            if ($bit) {
                $byte |= (1 << $bitIndex);
            }

            $bitIndex++;

            if ($bitIndex === 8) {
                $this->buffer .= \chr($byte);
                $byte = 0;
                $bitIndex = 0;
            }
        }

        // If there are remaining bits that don't fill a byte
        if ($bitIndex > 0) {
            $this->buffer .= \chr($byte);
        }
    }

    public function writeBooleans(array $values) : void
    {
        $bits = [];

        foreach ($values as $value) {
            $bits[] = $value ? 1 : 0;
        }
        $this->writeBits($bits);
    }

    public function writeBytes(array $bytes) : void
    {
        foreach ($bytes as $byte) {
            $this->buffer .= \chr($byte);
        }
    }

    public function writeDecimals(array $decimals, int $byteLength, int $precision = 10, int $scale = 2) : void
    {
        $isBigEndian = $this->byteOrder === ByteOrder::BIG_ENDIAN;

        foreach ($decimals as $decimal) {
            $decimal = (int) \number_format($decimal, $scale, '', '');
            $bytes = [];

            for ($i = $byteLength - 1; $i >= 0; $i--) {
                $shift = $i * 8;
                $bytes[] = ($decimal >> $shift) & 0xFF;
            }

            if ($isBigEndian) {
                $bytes = \array_reverse($bytes);  // Reverse the byte order for big endian
            }

            $packedBytes = '';

            foreach ($bytes as $byte) {
                $packedBytes .= \pack('C', $byte);  // Pack each byte individually
            }

            $this->buffer .= $packedBytes;
        }
    }

    public function writeDoubles(array $doubles) : void
    {
        $format = $this->byteOrder === ByteOrder::BIG_ENDIAN ? 'E' : 'e';

        foreach ($doubles as $double) {
            $this->buffer .= \pack($format, $double);
        }
    }

    public function writeFloats(array $floats) : void
    {
        $format = $this->byteOrder === ByteOrder::BIG_ENDIAN ? 'G' : 'g';

        foreach ($floats as $float) {
            $this->buffer .= \pack($format, $float);
        }
    }

    public function writeInts32(array $ints) : void
    {
        $format = $this->byteOrder === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

        foreach ($ints as $int) {
            $this->buffer .= \pack($format, $int);
        }
    }

    public function writeInts64(array $ints) : void
    {
        $format = $this->byteOrder === ByteOrder::BIG_ENDIAN ? 'J' : 'P';

        foreach ($ints as $int) {
            $this->buffer .= \pack($format, $int);
        }
    }

    /**
     * @param array<string> $strings
     */
    public function writeStrings(array $strings) : void
    {
        $format = $this->byteOrder === ByteOrder::BIG_ENDIAN ? 'N' : 'V';

        foreach ($strings as $string) {
            $length = \strlen($string);
            $this->buffer .= \pack($format, $length);
            $this->buffer .= $string;
        }
    }

    public function writeVarInts32(array $values) : void
    {
        foreach ($values as $value) {
            if ($value < 128) {
                $this->buffer .= \chr($value);
            } else {
                do {
                    $temp = $value & 0x7F;
                    $value >>= 7;

                    if ($value) {
                        $temp |= 0x80;
                    }
                    $this->buffer .= \chr($temp);
                } while ($value);
            }
        }
    }
}
