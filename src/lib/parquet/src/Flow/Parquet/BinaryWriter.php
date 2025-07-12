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
     * @param array<bool> $values
     */
    public function writeBooleans(array $values) : void;

    /**
     * @param array<int> $bytes
     */
    public function writeBytes(array $bytes) : void;

    /**
     * @param array<float> $decimals
     */
    public function writeDecimals(array $decimals, int $byteLength, int $precision = 10, int $scale = 2) : void;

    /**
     * @param array<float> $doubles
     */
    public function writeDoubles(array $doubles) : void;

    /**
     * @param array<float> $floats
     */
    public function writeFloats(array $floats) : void;

    /**
     * @param array<int> $ints
     */
    public function writeInts16(array $ints) : void;

    /**
     * @param array<int> $ints
     */
    public function writeInts32(array $ints) : void;

    /**
     * @param array<int> $ints
     */
    public function writeInts64(array $ints) : void;

    /**
     * @param array<string> $strings
     */
    public function writeStrings(array $strings) : void;

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
