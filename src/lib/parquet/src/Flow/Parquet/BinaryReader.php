<?php declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\BinaryReader\Bytes;

interface BinaryReader
{
    public function length() : DataSize;

    public function position() : DataSize;

    /**
     * @return array<int>
     */
    public function readBits(int $total) : array;

    /**
     * @return array<bool>
     */
    public function readBooleans(int $total) : array;

    /**
     * @return array<array<int>>
     */
    public function readByteArrays(int $total) : array;

    public function readBytes(int $total) : Bytes;

    public function readDecimals(int $total, int $byteLength, int $precision = 10, int $scale = 2) : array;

    /**
     * @return array<float>
     */
    public function readDoubles(int $total) : array;

    /**
     * @return array<float>
     */
    public function readFloats(int $total) : array;

    /**
     * @return array<int>
     */
    public function readInts32(int $total) : array;

    /**
     * @return array<int>
     */
    public function readInts64(int $total) : array;

    /**
     * @return array<array<int>>
     */
    public function readInts96(int $total) : array;

    public function readStrings(int $total) : array;

    /**
     * @return array<int>
     */
    public function readUInts32(int $total) : array;

    /**
     * @return array<int>
     */
    public function readUInts64(int $total) : array;

    public function readVarInt() : int;

    public function remainingLength() : DataSize;

    public function seekBits(int $bits) : void;

    public function seekBytes(int $bytes) : void;
}
