<?php declare(strict_types=1);

namespace Flow\Parquet;

interface BinaryWriter
{
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
     * @param array<int> $values
     */
    public function writeVarInts32(array $values) : void;
}
