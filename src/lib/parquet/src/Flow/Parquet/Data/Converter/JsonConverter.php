<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;

final class JsonConverter implements Converter
{
    public function fromParquetType(mixed $data) : string
    {
        if (!\is_string($data)) {
            throw new RuntimeException('Json must be read as a string from Parquet file');
        }

        return $data;
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->logicalType()?->name() === LogicalType::JSON) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : string
    {
        if (!\is_string($data)) {
            throw new RuntimeException('Json must be written as a string from Parquet file');
        }

        return $data;
    }
}
