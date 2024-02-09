<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryReader;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class PlainValueUnpacker
{
    public function __construct(private readonly BinaryReader $reader)
    {

    }

    /**
     * @psalm-suppress PossiblyNullArgument
     *
     * @return array<mixed>
     */
    public function unpack(FlatColumn $column, int $total) : array
    {
        return match ($column->type()) {
            PhysicalType::INT32 => $this->reader->readInts32($total),
            PhysicalType::INT64 => $this->reader->readInts64($total),
            PhysicalType::INT96 => $this->reader->readInts96($total),
            PhysicalType::FLOAT => $this->reader->readFloats($total),
            PhysicalType::DOUBLE => $this->reader->readDoubles($total),
            PhysicalType::BYTE_ARRAY => match ($column->logicalType()?->name()) {
                LogicalType::STRING, LogicalType::JSON, LogicalType::UUID => $this->reader->readStrings($total),
                default => $this->reader->readByteArrays($total)
            },
            PhysicalType::FIXED_LEN_BYTE_ARRAY => match ($column->logicalType()?->name()) {
                /** @phpstan-ignore-next-line */
                LogicalType::DECIMAL => $this->reader->readDecimals($total, $column->typeLength(), $column->logicalType()?->decimalData()?->precision(), $column->logicalType()?->decimalData()?->scale()),
                default => throw new RuntimeException('Unsupported logical type ' . ($column->logicalType()?->name() ?: 'null') . ' for FIXED_LEN_BYTE_ARRAY'),
            },
            PhysicalType::BOOLEAN => $this->reader->readBooleans($total),
        };
    }
}
