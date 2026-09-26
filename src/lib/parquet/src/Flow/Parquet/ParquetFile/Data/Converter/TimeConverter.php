<?php

declare(strict_types=1);

namespace Flow\Parquet\ParquetFile\Data\Converter;

use DateInterval;
use DateTimeImmutable;
use DateTimeZone;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Data\Converter;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;

use function Flow\Parquet\floor_div;
use function get_debug_type;
use function is_int;
use function sprintf;

final readonly class TimeConverter implements Converter
{
    public function __construct(
        private TimeUnit $unit,
    ) {}

    public static function forColumn(FlatColumn $column, Options $options): ?self
    {
        $time = $column->logicalType()?->timeData();

        return ($column->type() === PhysicalType::INT32 || $column->type() === PhysicalType::INT64) && $time !== null
            ? new self($time->unit())
            : null;
    }

    public function fromParquetType(mixed $data): DateInterval
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return $this->toDateInterval(match ($this->unit) {
            TimeUnit::MILLISECONDS => $data * 1_000,
            TimeUnit::MICROSECONDS => $data,
            TimeUnit::NANOSECONDS => floor_div($data, 1_000),
        });
    }

    public function toParquetType(mixed $data): int
    {
        if (!$data instanceof DateInterval) {
            throw new InvalidArgumentException(sprintf('Expected DateInterval, got %s', get_debug_type($data)));
        }

        $micros = $this->toInt($data);

        return match ($this->unit) {
            TimeUnit::MILLISECONDS => floor_div($micros, 1_000),
            TimeUnit::MICROSECONDS => $micros,
            TimeUnit::NANOSECONDS => $micros * 1_000,
        };
    }

    private function toDateInterval(int $microseconds): DateInterval
    {
        $base = new DateTimeImmutable('1970-01-01 00:00:00.000000', new DateTimeZone('UTC'));
        $target = $base->modify(sprintf('+%d microseconds', $microseconds));

        return $base->diff($target);
    }

    private function toInt(DateInterval $interval): int
    {
        if ($interval->y !== 0) {
            throw new InvalidArgumentException(
                'The DateInterval object contains years, cannot convert to microseconds to represent time.',
            );
        }

        if ($interval->m !== 0) {
            throw new InvalidArgumentException(
                'The DateInterval object contains months, cannot convert to microseconds to represent time.',
            );
        }

        $microseconds = 0;

        $microseconds += $interval->y * 365 * 24 * 60 * 60 * 1000000; // years to microseconds
        $microseconds += $interval->m * 30 * 24 * 60 * 60 * 1000000; // months to microseconds (approx)
        $microseconds += $interval->d * 24 * 60 * 60 * 1000000; // days to microseconds
        $microseconds += $interval->h * 60 * 60 * 1000000; // hours to microseconds
        $microseconds += $interval->i * 60 * 1000000; // minutes to microseconds
        $microseconds += $interval->s * 1000000; // seconds to microseconds
        $microseconds += (int) ($interval->f * 1000000); // microseconds

        return $microseconds;
    }
}
