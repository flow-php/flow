<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\BinaryWriter\BinaryBufferWriter;
use Flow\Parquet\Data\DataConverter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class PlainValuesPacker
{
    public function __construct(
        private readonly DataConverter $dataConverter,
    ) {
    }

    public function packValues(FlatColumn $column, array $values) : string
    {
        $valuesBuffer = '';
        $parquetValues = [];

        foreach ($values as $value) {
            $parquetValues[] = $this->dataConverter->toParquetType($column, $value);
        }

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                (new BinaryBufferWriter($valuesBuffer))->writeBooleans($parquetValues);

                break;
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts32($parquetValues);

                        break;
                    case null:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts32($parquetValues);

                        break;
                }

                break;
            case PhysicalType::INT64:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts64($parquetValues);

                        break;
                    case null:
                        (new BinaryBufferWriter($valuesBuffer))->writeInts64($parquetValues);

                        break;
                }

                break;
            case PhysicalType::FLOAT:
                (new BinaryBufferWriter($valuesBuffer))->writeFloats($parquetValues);

                break;
            case PhysicalType::DOUBLE:
                (new BinaryBufferWriter($valuesBuffer))->writeDoubles($parquetValues);

                break;
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DECIMAL:
                        /**
                         * @phpstan-ignore-next-line
                         *
                         * @psalm-suppress PossiblyNullArgument
                         */
                        (new BinaryBufferWriter($valuesBuffer))->writeDecimals($parquetValues, $column->typeLength(), $column->precision(), $column->scale());

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
                        (new BinaryBufferWriter($valuesBuffer))->writeStrings($parquetValues);

                        break;

                    default:
                        throw new \RuntimeException('Writing logical type "' . ($column->logicalType()?->name() ?: 'UNKNOWN') . '" is not implemented yet');
                }

                break;

            default:
                throw new \RuntimeException('Writing physical type "' . $column->type()->name . '" is not implemented yet');
        }

        return $valuesBuffer;
    }
}
