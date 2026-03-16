<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class Int64DateTimeConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateTimeImmutable
    {
        /** @var int $data */
        return new \DateTimeImmutable('@' . \number_format($data / 1_000_000, 6, '.', ''));
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT64 && $column->logicalType()?->name() === LogicalType::TIMESTAMP) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : int
    {
        /** @var \DateTimeInterface $data */
        return $data->getTimestamp() * 1_000_000 + (int) $data->format('u');
    }
}
