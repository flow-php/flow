<?php

declare(strict_types=1);

namespace Flow\Parquet;

interface BinaryWriter
{
    public function append(string $buffer) : void;

    public function length() : DataSize;

    /**
     * @param array<int> $bits
     */
    public function writeBits(array $bits) : void;

    /**
     * @param array<int> $bytes
     */
    public function writeBytes(array $bytes) : void;

    /**
     * Write values using variable-length encoding (VarInt/ULEB128).
     *
     * VarInt encoding uses the most significant bit of each byte as a continuation bit.
     * Values are encoded in little-endian byte order with 7 bits of data per byte.
     * Negative values are treated as unsigned 64-bit integers for proper encoding.
     * This method handles all edge cases including overflow and ZigZag encoded values.
     *
     * @param array<int> $values
     */
    public function writeVarInts(array $values) : void;
}
