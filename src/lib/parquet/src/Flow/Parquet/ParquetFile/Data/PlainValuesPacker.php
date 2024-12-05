<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class PlainValuesPacker
{
    public function __construct(private readonly BinaryWriter $writer)
    {
    }

    public function packValues(FlatColumn $column, array $values) : void
    {
        $values = \array_filter($values, static fn (mixed $value) => $value !== null);

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                $this->writer->writeBooleans($values);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                    case null:
                        $this->writer->writeInts32($values);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                    case null:
                        $this->writer->writeInts64($values);

                        break;
                }

                break;
            case PhysicalType::FLOAT:
                $this->writer->writeFloats($values);

                break;
            case PhysicalType::DOUBLE:
                $this->writer->writeDoubles($values);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::UUID:
                        $this->writer->writeStrings($values);

                        break;
                    case LogicalType::DECIMAL:
                        /**
                         * @psalm-suppress PossiblyNullArgument
                         */
                        $this->writer->writeDecimals($values, $column->typeLength(), $column->precision(), $column->scale());

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::JSON:
                    case LogicalType::STRING:
                        $this->writer->writeStrings($values);

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;

            default:
                throw new \RuntimeException('Writing physical type "' . $column->type()->name . '" is not implemented yet');
        }
    }
}
