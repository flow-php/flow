<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data;

use Flow\Parquet\BinaryWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class PlainValuesPacker
{
    public function __construct(
        private readonly BinaryWriter $writer,
        private readonly DataConverter $dataConverter,
    ) {
    }

    public function packValues(FlatColumn $column, array $values) : void
    {
        $parquetValues = [];

        foreach ($values as $value) {
            if ($value === null) {
                continue;
            }

            $parquetValues[] = $this->dataConverter->toParquetType($column, $value);
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                $this->writer->writeBooleans($parquetValues);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                        $this->writer->writeInts32($parquetValues);

                        break;
                    case null:
                        $this->writer->writeInts32($parquetValues);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        $this->writer->writeInts64($parquetValues);

                        break;
                    case null:
                        $this->writer->writeInts64($parquetValues);

                        break;
                }

                break;
            case PhysicalType::FLOAT:
                $this->writer->writeFloats($parquetValues);

                break;
            case PhysicalType::DOUBLE:
                $this->writer->writeDoubles($parquetValues);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DECIMAL:
                        /**
                         * @phpstan-ignore-next-line
                         *
                         * @psalm-suppress PossiblyNullArgument
                         */
                        $this->writer->writeDecimals($parquetValues, $column->typeLength(), $column->precision(), $column->scale());

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::JSON:
                    case LogicalType::UUID:
                    case LogicalType::STRING:
                        $this->writer->writeStrings($parquetValues);

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
