<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\{ConvertedType, FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\{BinaryReader, Option, Options};

final class PlainValueUnpacker
{
    public function __construct(
        private readonly BinaryReader $reader,
        private readonly Options $options,
    ) {

    }

    /**
     * @return array<mixed>
     */
    public function unpack(FlatColumn $column, int $total) : array
    {
        if ($total === 0) {
            return [];
        }

        return match ($column->type()) {
            PhysicalType::INT32 => match ($column->convertedType()) {
                ConvertedType::INT_16 => $this->reader->readInts16($total),
                default => $this->reader->readInts32($total),
            },
            PhysicalType::INT64 => $this->reader->readInts64($total),
            PhysicalType::INT96 => $this->reader->readInts96($total),
            PhysicalType::FLOAT => $this->reader->readFloats($total),
            PhysicalType::DOUBLE => $this->reader->readDoubles($total),
            PhysicalType::BYTE_ARRAY => match ($column->logicalType()?->name()) {
                LogicalType::STRING, LogicalType::JSON, LogicalType::UUID => $this->reader->readStrings($total),
                default => $this->options->get(Option::BYTE_ARRAY_TO_STRING)
                    ? $this->reader->readStrings($total)
                    : $this->reader->readByteArrays($total),
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => match ($column->logicalType()?->name()) {
                /** @phpstan-ignore-next-line */
                LogicalType::DECIMAL => $this->reader->readDecimals($total, $column->typeLength(), $column->logicalType()?->decimalData()?->precision(), $column->logicalType()?->decimalData()?->scale()),
                LogicalType::UUID => $this->reader->readStrings($total),
                default => throw new RuntimeException('Unsupported logical type ' . ($column->logicalType()?->name() ?: 'null') . ' for FIXED_LEN_BYTE_ARRAY'),
            },
            PhysicalType::BOOLEAN => $this->reader->readBooleans($total),
        };
    }
}
