<?php

declare(strict_types=1);

namespace Flow\Parquet;

interface BinaryReader
{
    public function length(): DataSize;

    public function position(): DataSize;

    /**
     * @return \Generator<int>
     */
    public function readBits(int $total): \Generator;

    public function readBytes(int $total): string;

    public function readVarInt(): int;

    public function remainingLength(): DataSize;

    public function seekBits(int $bits): void;

    public function seekBytes(int $bytes): void;
}
