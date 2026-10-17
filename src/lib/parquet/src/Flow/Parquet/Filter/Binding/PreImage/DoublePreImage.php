<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;

use function abs;
use function is_float;
use function is_int;

final readonly class DoublePreImage implements PreImage
{
    private const int EXACT_FLOAT_INTEGER = 2 ** 53;

    /**
     * The literal as the double it compares with, -0.0 as +0.0; null for a kind that is no number or an int a double
     * cannot hold exactly.
     */
    public static function value(bool|DateTimeImmutable|float|int|string $literal): ?float
    {
        return match (true) {
            is_int($literal) => abs($literal) <= self::EXACT_FLOAT_INTEGER ? (float) $literal : null,
            is_float($literal) => $literal === 0.0 ? 0.0 : $literal,
            default => null,
        };
    }

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        $value = self::value($literal);

        return $value === null ? null : EqualRange::point($value);
    }
}
