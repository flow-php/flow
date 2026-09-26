<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use DateTimeImmutable;
use DateTimeInterface;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;

use function Flow\Parquet\floor_div;
use function intdiv;
use function sprintf;

final readonly class Int64DateTimeConverter implements Converter
{
    public function __construct(
        private TimeUnit $unit,
    ) {}

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $timestamp = $column->logicalType()?->timestampData();

        return $column->type() === PhysicalType::INT64 && $timestamp !== null ? new self($timestamp->unit()) : null;
    }

    public function fromParquetType(mixed $data): DateTimeImmutable
    {
        /** @var int $data */
        $totalMicros = match ($this->unit) {
            TimeUnit::MILLISECONDS => $data * 1_000,
            TimeUnit::MICROSECONDS => $data,
            TimeUnit::NANOSECONDS => floor_div($data, 1_000),
        };
        $seconds = floor_div($totalMicros, 1_000_000);

        $dateTime = DateTimeImmutable::createFromFormat('U.u', sprintf(
            '%d.%06d',
            $seconds,
            $totalMicros - ($seconds * 1_000_000),
        ));

        if ($dateTime === false) {
            throw new RuntimeException('Failed to convert INT64 to DateTime, given: ' . $data);
        }

        return $dateTime;
    }

    public function toParquetType(mixed $data): int
    {
        /** @var DateTimeInterface $data */
        $totalMicros = ($data->getTimestamp() * 1_000_000) + (int) $data->format('u');

        if (
            $this->unit === TimeUnit::NANOSECONDS
            && ($totalMicros > intdiv(PHP_INT_MAX, 1_000) || $totalMicros < intdiv(PHP_INT_MIN, 1_000))
        ) {
            throw new InvalidArgumentException(sprintf(
                'DateTime %s is outside the TIMESTAMP(NANOS) range 1677-09-21 – 2262-04-11',
                $data->format(DATE_ATOM),
            ));
        }

        return match ($this->unit) {
            TimeUnit::MILLISECONDS => floor_div($totalMicros, 1_000),
            TimeUnit::MICROSECONDS => $totalMicros,
            TimeUnit::NANOSECONDS => $totalMicros * 1_000,
        };
    }
}
