<?php

declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use function Flow\Types\DSL\{type_instance_of, type_integer};
use Flow\Parquet\Data\Converter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\{FlatColumn, LogicalType, PhysicalType};

final class Int64DateTimeConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateTimeImmutable
    {
        return $this->microsecondsToDateTimeImmutable(type_integer()->assert($data));
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
        return $this->dateTimeToMicroseconds(type_instance_of(\DateTimeInterface::class)->assert($data));
    }

    private function dateTimeToMicroseconds(\DateTimeInterface $dateTime) : int
    {
        return (int) \bcadd(\bcmul($dateTime->format('U'), '1000000'), $dateTime->format('u'));
    }

    private function microsecondsToDateTimeImmutable(int $microseconds) : \DateTimeImmutable
    {
        return new \DateTimeImmutable('@' . \number_format($microseconds / 1_000_000, 6, '.', ''));
    }
}
