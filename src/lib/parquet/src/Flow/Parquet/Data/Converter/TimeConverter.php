<?php declare(strict_types=1);

namespace Flow\Parquet\Data\Converter;

use Flow\Parquet\Data\Converter;
use Flow\Parquet\Exception\InvalidArgumentException;
use Flow\Parquet\Options;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

final class TimeConverter implements Converter
{
    public function fromParquetType(mixed $data) : \DateInterval
    {
        return $this->toDateInterval($data);
    }

    public function isFor(FlatColumn $column, Options $options) : bool
    {
        if ($column->type() === PhysicalType::INT32 && $column->logicalType()?->name() === LogicalType::TIME) {
            return true;
        }

        return false;
    }

    public function toParquetType(mixed $data) : int
    {
        return $this->toInt($data);
    }

    /**
     * @psalm-suppress InaccessibleProperty
     */
    private function toDateInterval(int $microseconds) : \DateInterval
    {
        $seconds = (int) \floor($microseconds / 1000000);
        $remainingMicroseconds = $microseconds % 1000000;

        $minutes = (int) \floor($seconds / 60);
        $remainingSeconds = $seconds % 60;

        $hours = (int) \floor($minutes / 60);
        $remainingMinutes = $minutes % 60;

        $days = (int) \floor($hours / 24);
        $remainingHours = $hours % 24;

        $months = (int) \floor($days / 30); // Approximation

        if ($months !== 0) {
            throw new InvalidArgumentException('The DateInterval object contains months, cannot convert to microseconds to represent time.');
        }

        $intervalSpec = \sprintf(
            'PT%dH%dM%dS',
            $remainingHours,
            $remainingMinutes,
            $remainingSeconds
        );

        $interval = new \DateInterval($intervalSpec);
        $interval->y = 0;
        $interval->m = 0;
        $interval->d = 0;
        $interval->f = ($remainingMicroseconds / 1000000);

        return $interval;
    }

    private function toInt(\DateInterval $interval) : int
    {
        if ($interval->y !== 0) {
            throw new InvalidArgumentException('The DateInterval object contains years, cannot convert to microseconds to represent time.');
        }

        if ($interval->m !== 0) {
            throw new InvalidArgumentException('The DateInterval object contains months, cannot convert to microseconds to represent time.');
        }

        $microseconds = 0;

        $microseconds += $interval->y * 365 * 24 * 60 * 60 * 1000000; // years to microseconds
        $microseconds += $interval->m * 30 * 24 * 60 * 60 * 1000000; // months to microseconds (approx)
        $microseconds += $interval->d * 24 * 60 * 60 * 1000000; // days to microseconds
        $microseconds += $interval->h * 60 * 60 * 1000000; // hours to microseconds
        $microseconds += $interval->i * 60 * 1000000; // minutes to microseconds
        $microseconds += $interval->s * 1000000; // seconds to microseconds
        $microseconds += (int) (($interval->f) * 1000000); // microseconds

        return $microseconds;
    }
}
