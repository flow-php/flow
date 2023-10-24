<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class Int32DateConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateTimeImmutable
    {
        return $this->numberOfDaysToDateTime($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT32 && $column->logicalType()?->name() === LogicalType::DATE) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : int
    {
        return $this->dateTimeToNumberOfDays($data);
    }

    private function dateTimeToNumberOfDays(\DateTime|\DateTimeImmutable $date) : int
    {
        $epoch = new \DateTimeImmutable('1970-01-01 00:00:00 UTC');
        $interval = $epoch->diff($date->setTime(0, 0, 0, 0));

        return (int) $interval->format('%a');
    }

    private function numberOfDaysToDateTime(int $data) : \DateTimeImmutable
    {
        return (new \DateTimeImmutable('1970-01-01 00:00:00 UTC'))->add(new \DateInterval('P' . $data . 'D'));
    }
}
