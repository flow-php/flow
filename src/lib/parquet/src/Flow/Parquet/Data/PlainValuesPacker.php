<?php

declare(strict_types=1);

namespace Flow\Parquet\Data;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final readonly class PlainValuesPacker
{
    public function __construct(private BinaryWriter $writer)
    {
    }

    /**
     * @param array<mixed> $values
     */
    public function packValues(FlatColumn $column, array $values) : void
    {
        $values = \array_filter($values, static fn (mixed $value) => $value !== null);

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                /** @phpstan-ignore-next-line */
                $this->writer->writeBooleans($values);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                    case null:
                        /** @phpstan-ignore-next-line */
                        $this->writer->writeInts32($values);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                    case null:
                        /** @phpstan-ignore-next-line */
                        $this->writer->writeInts64($values);

                        break;
                }

                break;
            case PhysicalType::FLOAT:
                /** @phpstan-ignore-next-line */
                $this->writer->writeFloats($values);

                break;
            case PhysicalType::DOUBLE:
                /** @phpstan-ignore-next-line */
                $this->writer->writeDoubles($values);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                match ($column->logicalType()?->name()) {
                    /** @phpstan-ignore-next-line */
                    LogicalType::UUID => $this->writer->writeStrings($values),
                    /** @phpstan-ignore-next-line */
                    LogicalType::DECIMAL => $this->writer->writeDecimals($values, (int) $column->typeLength(), (int) $column->precision(), (int) $column->scale()),
                    default => throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet'),
                };

                break;
            case PhysicalType::BYTE_ARRAY:
                match ($column->logicalType()?->name()) {
                    /** @phpstan-ignore-next-line */
                    LogicalType::JSON, LogicalType::STRING => $this->writer->writeStrings($values),
                    default => throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet'),
                };

                break;

            default:
                throw new \RuntimeException('Writing physical type "' . $column->type()->name . '" is not implemented yet');
        }
    }
}
