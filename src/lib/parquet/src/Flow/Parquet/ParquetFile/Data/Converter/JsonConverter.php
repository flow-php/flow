<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;

use function is_string;

final readonly class JsonConverter implements Converter
{
    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        return $column->logicalType()?->name() === LogicalType::JSON ? new self() : null;
    }

    public function fromParquetType(mixed $data): string
    {
        if (!is_string($data)) {
            throw new RuntimeException('Json must be read as a string from Parquet file');
        }

        return $data;
    }

    public function toParquetType(mixed $data): string
    {
        if (!is_string($data)) {
            throw new RuntimeException('Json must be written as a string from Parquet file');
        }

        return $data;
    }
}
