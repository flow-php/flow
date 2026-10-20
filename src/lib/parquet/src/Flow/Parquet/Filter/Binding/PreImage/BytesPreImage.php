<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PhysicalInterval;
use Flow\Parquet\Filter\Binding\PhysicalIntervals;
use Flow\Parquet\Filter\Binding\PreImage;

use function chr;
use function ord;
use function rtrim;
use function strlen;
use function substr;

final readonly class BytesPreImage implements PreImage
{
    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        /** @var string $literal LiteralDomain::Bytes holds only strings */
        return EqualRange::point($literal);
    }

    /**
     * The values from the prefix up to its increment: a prefix of only 0xFF bytes has no upper bound.
     */
    public function startsWith(string $prefix): PhysicalIntervals
    {
        if ($prefix === '') {
            return new PhysicalIntervals(PhysicalOrder::Bytes, [PhysicalInterval::unbounded()]);
        }

        $upper = rtrim($prefix, "\xFF");

        return new PhysicalIntervals(PhysicalOrder::Bytes, [
            $upper === ''
                ? new PhysicalInterval($prefix, true, null, false)
                : new PhysicalInterval(
                    $prefix,
                    true,
                    substr($upper, 0, -1) . chr(ord($upper[strlen($upper) - 1]) + 1),
                    false,
                ),
        ]);
    }
}
