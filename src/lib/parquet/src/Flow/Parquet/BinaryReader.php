<?php declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\BinaryReader\Bytes;

interface BinaryReader
{
    public function length() : DataSize;

    public function position() : DataSize;

    public function readBit() : int;

    /**
     * @return array<int>
     */
    public function readBits(int $total) : array;

    public function readBoolean() : bool;

    /**
     * @return array<bool>
     */
    public function readBooleans(int $total) : array;

    public function readByte() : int;

    /**
     * @return array<array<int>>
     */
    public function readByteArrays(int $total) : array;

    public function readBytes(int $total) : Bytes;

    public function readDecimals(int $total, int $byteLength) : array;

    public function readDouble() : float;

    /**
     * @return array<float>
     */
    public function readDoubles(int $total) : array;

    public function readFloat() : float;

    /**
     * @return array<float>
     */
    public function readFloats(int $total) : array;

    public function readInt32() : int;

    public function readInt64() : int;

    /**
     * @return array<int>
     */
    public function readInt96() : array;

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

    public function readString() : string;

    public function readStrings(int $total) : array;

    public function readUInt32() : int;

    public function readUInt64() : int;

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
