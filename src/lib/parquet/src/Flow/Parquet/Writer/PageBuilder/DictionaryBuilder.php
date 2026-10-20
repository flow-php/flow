<?php

declare(strict_types=1);

namespace Flow\Parquet\Writer\PageBuilder;

use Flow\Parquet\Dremel\ColumnData\WriteFlatColumnValues;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder\FloatDictionaryBuilder;
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder\ObjectDictionaryBuilder;
use Flow\Parquet\Writer\PageBuilder\DictionaryBuilder\ScalarDictionaryBuilder;
use RuntimeException;

final class DictionaryBuilder
{
    /**
     * $dictionary extended with the values of $data, its indices those of $data's values.
     */
    public function build(
        FlatColumn $column,
        WriteFlatColumnValues $data,
        Dictionary $dictionary = new Dictionary([], []),
    ): Dictionary {
        return match ($column->type()) {
            PhysicalType::INT64, PhysicalType::INT32 => match ($column->logicalType()?->name()) {
                LogicalType::DATE, LogicalType::TIME, LogicalType::TIMESTAMP => (new ObjectDictionaryBuilder())->build(
                    $data,
                    $dictionary,
                ),
                default => (new ScalarDictionaryBuilder())->build($data, $dictionary),
            },
            PhysicalType::BOOLEAN => (new ScalarDictionaryBuilder())->build($data, $dictionary),
            PhysicalType::FLOAT, PhysicalType::DOUBLE => (new FloatDictionaryBuilder())->build($data, $dictionary),
            PhysicalType::FIXED_LEN_BYTE_ARRAY, PhysicalType::BYTE_ARRAY => match ($column->logicalType()?->name()) {
                LogicalType::STRING,
                LogicalType::JSON,
                LogicalType::BSON,
                LogicalType::UUID,
                LogicalType::ENUM,
                LogicalType::DECIMAL,
                    => (new ScalarDictionaryBuilder())->build($data, $dictionary),
                LogicalType::DATE, LogicalType::TIME, LogicalType::TIMESTAMP => (new ObjectDictionaryBuilder())->build(
                    $data,
                    $dictionary,
                ),
                default => throw new RuntimeException(
                    'Building dictionary for "' . ($column->logicalType()?->name() ?? 'null') . '" is not supported',
                ),
            },
            default => throw new RuntimeException(
                'Building dictionary for "' . $column->type()->name . '" is not supported',
            ),
        };
    }
}
