<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use function Flow\Parquet\Binary\{decode_decimal, decode_f32, decode_f64, decode_i16, decode_i32, decode_i64, decode_u32};
use Flow\Parquet\Binary\ByteOrder;
use Flow\Parquet\BinaryReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\{ConvertedType, FlatColumn, LogicalType, PhysicalType};

final readonly class PlainValueUnpacker
{
    public function __construct(
        private BinaryReader $reader,
        private ByteOrder $byteOrder = ByteOrder::LITTLE_ENDIAN,
    ) {
    }

    /**
     * @return \Generator<mixed>
     */
    public function unpack(FlatColumn $column, int $total) : \Generator
    {
        if ($total === 0) {
            return;
        }

        yield from match ($column->type()) {
            PhysicalType::BOOLEAN => $this->unpackBooleans($total),
            PhysicalType::INT32 => $this->unpackInt32($column, $total),
            PhysicalType::INT64 => $this->unpackInt64($total),
            PhysicalType::INT96 => $this->unpackInt96($total),
            PhysicalType::FLOAT => $this->unpackFloats($total),
            PhysicalType::DOUBLE => $this->unpackDoubles($total),
            PhysicalType::BYTE_ARRAY => $this->unpackByteArray($column, $total),
            PhysicalType::FIXED_LEN_BYTE_ARRAY => $this->unpackFixedLenByteArray($column, $total),
        };
    }

    private function rawBytesToUuidString(string $raw) : string
    {
        $hex = \bin2hex($raw);

        return \sprintf(
            '%s-%s-%s-%s-%s',
            \substr($hex, 0, 8),
            \substr($hex, 8, 4),
            \substr($hex, 12, 4),
            \substr($hex, 16, 4),
            \substr($hex, 20, 12)
        );
    }

    /**
     * @return \Generator<bool>
     */
    private function unpackBooleans(int $total) : \Generator
    {
        foreach ($this->reader->readBits($total) as $bit) {
            yield (bool) $bit;
        }
    }

    /**
     * @return \Generator<string>
     */
    private function unpackByteArray(FlatColumn $column, int $total) : \Generator
    {
        for ($i = 0; $i < $total; $i++) {
            $length = decode_u32($this->byteOrder, $this->reader->readBytes(4))[0];

            yield match ($column->logicalType()?->name()) {
                LogicalType::STRING, LogicalType::JSON, LogicalType::UUID => $this->reader->readBytes($length),
                default => $this->reader->readBytes($length),
            };
        }
    }

    /**
     * @return \Generator<float>
     */
    private function unpackDoubles(int $total) : \Generator
    {
        yield from decode_f64($this->byteOrder, $this->reader->readBytes($total * 8));
    }

    /**
     * @return \Generator<float|string>
     */
    private function unpackFixedLenByteArray(FlatColumn $column, int $total) : \Generator
    {
        $typeLength = $column->typeLength();

        if ($typeLength === null) {
            throw new RuntimeException('FIXED_LEN_BYTE_ARRAY requires typeLength in schema');
        }

        $logicalType = $column->logicalType();
        $decimalData = $logicalType?->decimalData();

        for ($i = 0; $i < $total; $i++) {
            $raw = $this->reader->readBytes($typeLength);

            yield match ($logicalType?->name()) {
                LogicalType::DECIMAL => decode_decimal(
                    $this->byteOrder,
                    $raw,
                    $decimalData?->precision() ?? 10,
                    $decimalData?->scale() ?? 0
                ),
                LogicalType::UUID => $this->rawBytesToUuidString($raw),
                default => $raw,
            };
        }
    }

    /**
     * @return \Generator<float>
     */
    private function unpackFloats(int $total) : \Generator
    {
        yield from decode_f32($this->byteOrder, $this->reader->readBytes($total * 4));
    }

    /**
     * @return \Generator<int>
     */
    private function unpackInt32(FlatColumn $column, int $total) : \Generator
    {
        if ($column->convertedType() === ConvertedType::INT_16) {
            yield from decode_i16($this->byteOrder, $this->reader->readBytes($total * 2));

            return;
        }

        yield from decode_i32($this->byteOrder, $this->reader->readBytes($total * 4));
    }

    /**
     * @return \Generator<int>
     */
    private function unpackInt64(int $total) : \Generator
    {
        yield from decode_i64($this->byteOrder, $this->reader->readBytes($total * 8));
    }

    /**
     * @return \Generator<string>
     */
    private function unpackInt96(int $total) : \Generator
    {
        for ($i = 0; $i < $total; $i++) {
            yield $this->reader->readBytes(12);
        }
    }
}
