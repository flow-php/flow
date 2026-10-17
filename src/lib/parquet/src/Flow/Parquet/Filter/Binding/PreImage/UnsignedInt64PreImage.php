<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\OutOfDomain;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;

use function abs;
use function ceil;
use function floor;
use function is_float;
use function is_int;
use function is_string;
use function preg_match;

/**
 * UINT_64 decodes to a digit string; its physical value is the INT64 bit pattern, ordered unsigned. A float compares
 * numerically, a fraction lying between two integers.
 */
final readonly class UnsignedInt64PreImage implements PreImage
{
    public function __construct(
        private UnsignedInt64Converter $converter,
    ) {}

    private const int EXACT_FLOAT_INTEGER = 2 ** 53;

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        if (is_float($literal)) {
            return abs($literal) > self::EXACT_FLOAT_INTEGER
                ? null
                : new EqualRange($this->edge((int) ceil($literal)), $this->edge((int) floor($literal)));
        }

        return match (true) {
            UnsignedInt64Converter::accepts($literal) => EqualRange::point($this->converter->toParquetType($literal)),
            is_int($literal) => EqualRange::point(OutOfDomain::Below),
            is_string($literal) && preg_match('/^[1-9][0-9]*$/', $literal) === 1 => EqualRange::point(
                OutOfDomain::Above,
            ),
            default => null,
        };
    }

    private function edge(int $value): int|OutOfDomain
    {
        return $value < 0 ? OutOfDomain::Below : $this->converter->toParquetType($value);
    }
}
