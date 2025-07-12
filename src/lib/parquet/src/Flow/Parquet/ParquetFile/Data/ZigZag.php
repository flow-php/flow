<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

final readonly class ZigZag
{
    /**
     * Decode a ZigZag encoded value back to signed integer.
     *
     * Implementation follows Protocol Buffers specification:
     * int64_t((n >> 1) ^ (~(n & 1) + 1))
     */
    public function decode(int $value) : int
    {
        // Protocol Buffers decode: (n >> 1) ^ (~(n & 1) + 1)
        // This is equivalent to: (n >> 1) ^ (-(n & 1))
        return ($value >> 1) ^ (-($value & 1));
    }

    /**
     * Encode a signed integer using ZigZag encoding.
     *
     * ZigZag encoding maps signed integers to unsigned integers so that
     * numbers with a small absolute value have a small encoded value.
     *
     * Implementation follows Protocol Buffers specification:
     * uint64_t(n << 1) ^ uint64_t(n >> 63)
     *
     * @see https://protobuf.dev/programming-guides/encoding/
     */
    public function encode(int $value) : int
    {
        if (PHP_INT_SIZE === 8) {
            // Standard ZigZag encoding for 64-bit integers
            // In PHP, we simulate unsigned arithmetic behavior
            // The operations naturally wrap around in 2's complement
            return ($value << 1) ^ ($value >> 63);
        }

        // Standard ZigZag encoding for 32-bit integers
        return ($value << 1) ^ ($value >> 31);
    }
}
