<?php

declare(strict_types=1);

namespace Flow\Parquet\BinaryWriter;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\DataSize;

use function bcadd;
use function bccomp;
use function bcdiv;
use function bcmod;
use function chr;
use function pack;
use function strlen;

final class BinaryBufferWriter implements BinaryWriter
{
    public function __construct(
        private string &$buffer,
    ) {
        $this->buffer = '';
    }

    public function append(string $buffer): void
    {
        $this->buffer .= $buffer;
    }

    public function length(): DataSize
    {
        return DataSize::fromBytes(strlen($this->buffer));
    }

    public function writeBits(array $bits): void
    {
        $byte = 0;
        $bitIndex = 0;

        foreach ($bits as $bit) {
            if ($bit) {
                $byte |= 1 << $bitIndex;
            }

            $bitIndex++;

            if ($bitIndex === 8) {
                $this->buffer .= chr($byte);
                $byte = 0;
                $bitIndex = 0;
            }
        }

        if ($bitIndex > 0) {
            $this->buffer .= chr($byte);
        }
    }

    public function writeBytes(array $bytes): void
    {
        $this->buffer .= pack('C*', ...$bytes);
    }

    public function writeVarInts(array $values): void
    {
        foreach ($values as $value) {
            $bytes = [];

            if ($value < 0) {
                $unsigned = bcadd((string) $value, '18446744073709551616', 0);

                while (bccomp($unsigned, '127', 0) > 0) {
                    $remainder = bcmod($unsigned, '128', 0);
                    $bytes[] = (int) $remainder | 0x80;
                    $unsigned = bcdiv($unsigned, '128', 0);
                }
                $bytes[] = (int) $unsigned;
            } else {
                while ($value >= 0x80) {
                    $bytes[] = ($value & 0x7F) | 0x80;
                    $value >>= 7;
                }
                $bytes[] = $value & 0x7F;
            }

            $this->writeBytes($bytes);
        }
    }
}
