<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Generator;

use function Flow\Parquet\Binary\decode_f32;
use function Flow\Parquet\Binary\decode_f64;
use function Flow\Parquet\Binary\decode_i32;
use function Flow\Parquet\Binary\decode_i64;
use function Flow\Parquet\Binary\decode_u32;

final readonly class PlainValueUnpacker
{
    public function __construct(
        private BinaryReader $reader,
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {}

    /**
     * @return \Generator<mixed>
     */
    public function unpack(FlatColumn $column, int $total): Generator
    {
        if ($total === 0) {
            return;
        }

        yield from match ($column->type()) {
            PhysicalType::BOOLEAN => $this->unpackBooleans($total),
            PhysicalType::INT32 => $this->unpackInt32($total),
            PhysicalType::INT64 => $this->unpackInt64($total),
            PhysicalType::INT96 => $this->unpackInt96($total),
            PhysicalType::FLOAT => $this->unpackFloats($total),
            PhysicalType::DOUBLE => $this->unpackDoubles($total),
            PhysicalType::BYTE_ARRAY => $this->unpackByteArray($total),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => $this->unpackFixedLenByteArray($column, $total),
        };
    }

    /**
     * @return \Generator<bool>
     */
    private function unpackBooleans(int $total): Generator
    {
        foreach ($this->reader->readBits($total) as $bit) {
            yield (bool) $bit;
        }
    }

    /**
     * @return \Generator<string>
     */
    private function unpackByteArray(int $total): Generator
    {
        for ($i = 0; $i < $total; $i++) {
            yield $this->reader->readBytes(decode_u32($this->byteOrder, $this->reader->readBytes(4))[0]);
        }
    }

    /**
     * @return \Generator<float>
     */
    private function unpackDoubles(int $total): Generator
    {
        yield from decode_f64($this->byteOrder, $this->reader->readBytes($total * 8));
    }

    /**
     * @return \Generator<string>
     */
    private function unpackFixedLenByteArray(FlatColumn $column, int $total): Generator
    {
        $typeLength = $column->typeLength();

        if ($typeLength === null) {
            throw new RuntimeException('FIXED_LEN_BYTE_ARRAY requires typeLength in schema');
        }

        for ($i = 0; $i < $total; $i++) {
            yield $this->reader->readBytes($typeLength);
        }
    }

    /**
     * @return \Generator<float>
     */
    private function unpackFloats(int $total): Generator
    {
        yield from decode_f32($this->byteOrder, $this->reader->readBytes($total * 4));
    }

    /**
     * @return \Generator<int>
     */
    private function unpackInt32(int $total): Generator
    {
        yield from decode_i32($this->byteOrder, $this->reader->readBytes($total * 4));
    }

    /**
     * @return \Generator<int>
     */
    private function unpackInt64(int $total): Generator
    {
        yield from decode_i64($this->byteOrder, $this->reader->readBytes($total * 8));
    }

    /**
     * @return \Generator<string>
     */
    private function unpackInt96(int $total): Generator
    {
        for ($i = 0; $i < $total; $i++) {
            yield $this->reader->readBytes(12);
        }
    }
}
