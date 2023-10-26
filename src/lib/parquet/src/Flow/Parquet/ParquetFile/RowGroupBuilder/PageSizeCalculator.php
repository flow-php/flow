<?php declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\RowGroupBuilder;

use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class PageSizeCalculator
{
    public function __construct(private readonly Options $options)
    {
    }

    public function rowsPerPage(FlatColumn $column, ColumnChunkStatistics $statistics) : int
    {
        $pageSize = (int) $this->options->get(Option::PAGE_SIZE_BYTES);

        switch ($column->type()) {
            case PhysicalType::BOOLEAN:
                // Booleans are stored as bits, so we can fit 8 of them into a single byte
                return $pageSize * 8;
            case PhysicalType::FLOAT:
            case PhysicalType::INT32:
                // Int32s are stored as 4 bytes
                return (int) \ceil($pageSize / 4);
            case PhysicalType::DOUBLE:
            case PhysicalType::INT64:
                // Int64s are stored as 8 bytes
                return (int) \ceil($pageSize / 8);
            case PhysicalType::INT96:
                // Int96s are stored as 12 bytes
                return (int) \ceil($pageSize / 12);
            case PhysicalType::FIXED_LEN_BYTE_ARRAY:
                // Fixed length byte arrays are stored as their length
                return (int) \floor($pageSize / $column->typeLength());
            case PhysicalType::BYTE_ARRAY:
                // Byte arrays (all string values) are stored as their length because we are anyway storing dictionary indices for them, not the actual values
                return $statistics->valuesCount();

            default:
                return $statistics->valuesCount();
        }
    }
}
