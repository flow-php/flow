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
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function get_debug_type;
use function is_int;
use function sprintf;

final class TimeConverter implements Converter
{
    public function fromParquetType(mixed $data): DateInterval
    {
        if (!is_int($data)) {
            throw new InvalidArgumentException(sprintf('Expected int, got %s', get_debug_type($data)));
        }

        return $this->toDateInterval($data);
    }

    public function isFor(FlatColumn $column, Options $options): bool
    {
        if ($column->type() === PhysicalType::INT64 && $column->logicalType()?->name() === LogicalType::TIME) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data): int
    {
        if (!$data instanceof DateInterval) {
            throw new InvalidArgumentException(sprintf('Expected DateInterval, got %s', get_debug_type($data)));
        }

        return $this->toInt($data);
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
