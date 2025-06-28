<?php

declare(strict_types=1);

namespace Flow\Parquet;

use Flow\Parquet\BinaryReader\Bytes;

interface BinaryReader
{
    public function length() : DataSize;

    public function position() : DataSize;

    /**
     * @return \Generator<int>
     */
    public function readBits(int $total) : \Generator;

    /**
     * @return \Generator<bool>
     */
    public function readBooleans(int $total) : \Generator;

    /**
     * @return \Generator<Bytes>
     */
    public function readByteArrays(int $total) : \Generator;

    public function readBytes(int $total) : Bytes;

    /**
     * @return \Generator<mixed>
     */
    public function readDecimals(int $total, int $byteLength, int $precision = 10, int $scale = 2) : \Generator;

    /**
     * @return \Generator<float>
     */
    public function readDoubles(int $total) : \Generator;

    /**
     * @return \Generator<float>
     */
    public function readFloats(int $total) : \Generator;

    /**
     * @return \Generator<int>
     */
    public function readInts16(int $total) : \Generator;

    /**
     * @return \Generator<int>
     */
    public function readInts32(int $total) : \Generator;

    /**
     * @return \Generator<int>
     */
    public function readInts64(int $total) : \Generator;

    /**
     * @return \Generator<Bytes>
     */
    public function readInts96(int $total) : \Generator;

    /**
     * @return \Generator<string>
     */
    public function readStrings(int $total) : \Generator;

    /**
     * @return \Generator<int>
     */
    public function readUInts32(int $total) : \Generator;

    /**
     * @return \Generator<int>
     */
    public function readUInts64(int $total) : \Generator;

    public function readVarInt() : int;

    public function remainingLength() : DataSize;

    public function seekBits(int $bits) : void;

    public function seekBytes(int $bytes) : void;
}
