<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder\{FloatDictionaryBuilder, ObjectDictionaryBuilder, ScalarDictionaryBuilder};

final class DictionaryBuilder
{
    public function build(FlatColumn $column, WriteFlatColumnValues $data) : Dictionary
    {
        switch ($column->type()) {
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                return match ($column->logicalType()?->name()) {
                    LogicalType::DATE, LogicalType::TIME, LogicalType::TIMESTAMP => (new ObjectDictionaryBuilder())->build($data),
                    default => (new ScalarDictionaryBuilder())->build($data),
                };
            case PhysicalType::BOOLEAN:
                return (new ScalarDictionaryBuilder())->build($data);
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                return (new FloatDictionaryBuilder())->build($data);
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
            case PhysicalType::BYTE_ARRAY:
                return match ($column->logicalType()?->name()) {
                    LogicalType::STRING, LogicalType::JSON, LogicalType::BSON, LogicalType::UUID, LogicalType::ENUM => (new ScalarDictionaryBuilder())->build($data),
                    LogicalType::DECIMAL => (new FloatDictionaryBuilder())->build($data),
                    LogicalType::DATE, LogicalType::TIME, LogicalType::TIMESTAMP => (new ObjectDictionaryBuilder())->build($data),
                    default => throw new \RuntimeException('Building dictionary for "' . $column->logicalType()?->name() . '" is not supported'),
                };

            default:
                throw new \RuntimeException('Building dictionary for "' . $column->type()->name . '" is not supported');
        }
    }
}
