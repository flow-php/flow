<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\TimeUnit;

use function bcadd;
use function bcmul;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

/**
 * Millis hit a micros literal only on the milli grid; nanos decode floored to micros.
 */
final readonly class TimestampPreImage implements PreImage
{
    public function __construct(
        private TimeUnit $unit,
    ) {}

    public static function forColumn(FlatColumn $column): ?self
    {
        $unit = $column->logicalType()?->timestampData()?->unit();

        return $unit === null ? null : new self($unit);
    }

    /**
     * @return numeric-string
     */
    public static function micros(DateTimeImmutable $literal): string
    {
        /** @var numeric-string $fraction */
        $fraction = $literal->format('u');

        return bcadd(bcmul((string) $literal->getTimestamp(), '1000000'), $fraction);
    }

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        if (!$literal instanceof DateTimeImmutable) {
            return null;
        }

        $micros = self::micros($literal);
        $nanos = bcmul($micros, '1000');

        return match ($this->unit) {
            TimeUnit::MILLISECONDS => EqualRange::grid($micros, '1000', PHP_INT_MIN, PHP_INT_MAX),
            TimeUnit::MICROSECONDS => EqualRange::integers($micros, $micros, PHP_INT_MIN, PHP_INT_MAX),
            TimeUnit::NANOSECONDS => EqualRange::integers($nanos, bcadd($nanos, '999'), PHP_INT_MIN, PHP_INT_MAX),
        };
    }
}
