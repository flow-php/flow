<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\ColumnData\FlatColumnValues;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder\{FloatDictionaryBuilder, ObjectDictionaryBuilder, ScalarDictionaryBuilder};
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class DictionaryBuilder
{
    public function build(FlatColumn $column, FlatColumnValues $data) : Dictionary
    {
        switch ($column->type()) {
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        return (new ObjectDictionaryBuilder())->build($data);
                }

                return (new ScalarDictionaryBuilder())->build($data);
            case PhysicalType::BOOLEAN:
                return (new ScalarDictionaryBuilder())->build($data);
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                return (new FloatDictionaryBuilder())->build($data);
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::BSON:
                    case LogicalType::UUID:
                    case LogicalType::ENUM:
                        return (new ScalarDictionaryBuilder())->build($data);
                    case LogicalType::DECIMAL:
                        return (new FloatDictionaryBuilder())->build($data);
                    case LogicalType::DATE:
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        return (new ObjectDictionaryBuilder())->build($data);
                }

                throw new \RuntimeException('Building dictionary for "' . $column->logicalType()?->name() . '" is not supported');

            default:
                throw new \RuntimeException('Building dictionary for "' . $column->type()->name . '" is not supported');
        }
    }
}
