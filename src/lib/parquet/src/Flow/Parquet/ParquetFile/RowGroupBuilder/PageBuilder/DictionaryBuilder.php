<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder;

use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder\FloatDictionaryBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder\ObjectDictionaryBuilder;
use Flow\Parquet\ParquetFile\RowGroupBuilder\PageBuilder\DictionaryBuilder\ScalarDictionaryBuilder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class DictionaryBuilder
{
    public function build(FlatColumn $column, array $rows) : Dictionary
    {
        switch ($column->type()) {
            case PhysicalType::INT64:
            case PhysicalType::INT32:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::DATE:
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        return (new ObjectDictionaryBuilder())->build($rows);
                }

                return (new ScalarDictionaryBuilder())->build($rows);
            case PhysicalType::BOOLEAN:
                return (new ScalarDictionaryBuilder())->build($rows);
            case PhysicalType::FLOAT:
            case PhysicalType::DOUBLE:
                return (new FloatDictionaryBuilder())->build($rows);
            case PhysicalType::BYTE_ARRAY:
                switch ($column->logicalType()?->name()) {
                    case LogicalType::STRING:
                    case LogicalType::JSON:
                    case LogicalType::BSON:
                    case LogicalType::UUID:
                    case LogicalType::ENUM:
                        return (new ScalarDictionaryBuilder())->build($rows);
                    case LogicalType::DECIMAL:
                        return (new FloatDictionaryBuilder())->build($rows);
                    case LogicalType::DATE:
                    case LogicalType::TIME:
                    case LogicalType::TIMESTAMP:
                        return (new ObjectDictionaryBuilder())->build($rows);
                }

                throw new \RuntimeException('Building dictionary for "' . $column->logicalType()?->name() . '" is not supported');

            default:
                throw new \RuntimeException('Building dictionary for "' . $column->type()->name . '" is not supported');
        }
    }
}
