<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class Int32DateTimeConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateTimeImmutable
    {
        return $this->millisecondsToDateTimeImmutable($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT32 && $column->logicalType()?->name() === LogicalType::TIMESTAMP) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : int
    {
        return $this->dateTimeToMicroseconds($data);
    }

    /**
     * @psalm-suppress ArgumentTypeCoercion
     */
    private function dateTimeToMicroseconds(\DateTimeInterface $dateTime) : int
    {
        $microseconds = \number_format((((int) $dateTime->format('u')) / 1000), 0, '', '') . '000';

        return (int) \bcadd(\bcmul($dateTime->format('U'), '1000000'), $microseconds);
    }

    private function millisecondsToDateTimeImmutable(int $microseconds) : \DateTimeImmutable
    {
        $seconds = ($microseconds / 1000000);
        $fraction = \str_pad((string) ($microseconds % 1000000), 6, '0', STR_PAD_LEFT);

        $dateTime = \DateTimeImmutable::createFromFormat('U.u', \sprintf('%d.%s', $seconds, $fraction));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT32 to DateTime, given microseconds: ' . \json_encode(['microseconds' => $microseconds, 'fraction' => $fraction], JSON_THROW_ON_ERROR));
        }

        return $dateTime;
    }
}
