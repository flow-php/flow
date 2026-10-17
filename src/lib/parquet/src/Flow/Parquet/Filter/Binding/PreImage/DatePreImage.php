<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;

/**
 * A day decodes to its UTC midnight: an instant off midnight equals no day.
 */
final readonly class DatePreImage implements PreImage
{
    private const string MICROS_PER_DAY = '86400000000';

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        return $literal instanceof DateTimeImmutable
            ? EqualRange::grid(TimestampPreImage::micros($literal), self::MICROS_PER_DAY, -2 ** 31, 2 ** 31 - 1)
            : null;
    }
}
