<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Exception\RuntimeException;
use Flow\Parquet\Option;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\ConvertedType;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;

use function bin2hex;
use function Flow\Parquet\floor_div;
use function get_debug_type;
use function intdiv;
use function is_int;
use function is_string;
use function sprintf;
use function strlen;
use function unpack;

final readonly class StoredTemporalConverter implements Converter
{
    private function __construct(
        private ?TimeUnit $unit,
        private bool $int96,
    ) {}

    public static function days(): self
    {
        return new self(null, false);
    }

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $timestamp = $column->logicalType()?->timestampData();
        $time = $column->logicalType()?->timeData();

        return match (true) {
            $column->type() === PhysicalType::INT32
                && (
                    $column->logicalType()?->name() === LogicalType::DATE
                    || $column->convertedType() === ConvertedType::DATE
                )
                => self::days(),
            $column->type() === PhysicalType::INT64 && $timestamp !== null => self::micros($timestamp->unit()),
            ($column->type() === PhysicalType::INT32 || $column->type() === PhysicalType::INT64) && $time !== null
                => self::micros($time->unit()),
            $column->type() === PhysicalType::INT96 && $options->get(Option::INT_96_AS_DATETIME) => self::int96(),
            default => null,
        };
    }

    public static function int96(): self
    {
        return new self(null, true);
    }

    public static function micros(TimeUnit $unit): self
    {
        return new self($unit, false);
    }

    public function fromParquetType(mixed $data): int
    {
        if ($this->int96) {
            return $this->int96Micros($data);
        }

        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return match ($this->unit) {
            null, TimeUnit::MICROSECONDS => $data,
            TimeUnit::MILLISECONDS => $data * 1_000,
            TimeUnit::NANOSECONDS => floor_div($data, 1_000),
        };
    }

    public function toParquetType(mixed $data): int
    {
        if ($this->int96) {
            throw new RuntimeException(
                'INT96 timestamps are read only, write microseconds since the epoch to an INT64 column',
            );
        }

        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        if (
            $this->unit === TimeUnit::NANOSECONDS
            && ($data > intdiv(PHP_INT_MAX, 1_000) || $data < intdiv(PHP_INT_MIN, 1_000))
        ) {
            throw new InvalidArgumentException(sprintf(
                '%d microseconds since the epoch is outside the TIMESTAMP(NANOS) range 1677-09-21 – 2262-04-11',
                $data,
            ));
        }

        return match ($this->unit) {
            null, TimeUnit::MICROSECONDS => $data,
            TimeUnit::MILLISECONDS => floor_div($data, 1_000),
            TimeUnit::NANOSECONDS => $data * 1_000,
        };
    }

    private function int96Micros(mixed $bytes): int
    {
        if (!is_string($bytes) || strlen($bytes) !== 12) {
            throw new InvalidArgumentException(sprintf('Expected 12 INT96 bytes, got %s', get_debug_type($bytes)));
        }

        /** @var array{nanos: int, days: int}|false $parts */
        $parts = unpack('Pnanos/Vdays', $bytes);

        if ($parts === false) {
            throw new RuntimeException('Failed to unpack INT96 bytes: ' . bin2hex($bytes));
        }

        return (($parts['days'] - 2_440_588) * 86_400_000_000) + floor_div($parts['nanos'], 1_000);
    }
}
