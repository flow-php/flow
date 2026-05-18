<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Stringable;

use function Flow\Types\DSL\type_string;
use function is_object;
use function is_string;
use function method_exists;

final class UuidConverter implements Converter
{
    public function fromParquetType(mixed $data): string
    {
        if (!is_string($data)) {
            throw new RuntimeException('UUID must be read as a string from Parquet file');
        }

        return $data;
    }

    public function isFor(FlatColumn $column, Options $options): bool
    {
        if ($column->logicalType()?->name() === LogicalType::UUID) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data): string
    {
        if (is_string($data)) {
            return $data;
        }

        if (is_object($data) && method_exists($data, 'toString')) {
            return type_string()->assert($data->toString());
        }

        if ($data instanceof Stringable) {
            return (string) $data;
        }

        throw new RuntimeException('UUID must be written as a string or Stringable object');
    }
}
