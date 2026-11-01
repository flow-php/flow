<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\LiteralDomain;
use Flow\Parquet\Filter\Binding\OutOfDomain;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Data\Converter\UnsignedInt64Converter;

use function abs;
use function ceil;
use function floor;
use function is_float;
use function is_int;

/**
 * Physical value is the INT64 bit pattern ordered unsigned; decoded value is a digit string.
 */
final readonly class UnsignedInt64PreImage implements PreImage
{
    public function __construct(
        private UnsignedInt64Converter $converter,
    ) {}

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        if (is_float($literal)) {
            return abs($literal) > LiteralDomain::EXACT_FLOAT_INTEGER
                ? null
                : new EqualRange($this->edge((int) ceil($literal)), $this->edge((int) floor($literal)));
        }

        // LiteralDomain::UnsignedInteger64 leaves an int or a canonical digit string: outside the type it lies below or above
        return match (true) {
            UnsignedInt64Converter::accepts($literal) => EqualRange::point($this->converter->toParquetType($literal)),
            is_int($literal) => EqualRange::point(OutOfDomain::Below),
            default => EqualRange::point(OutOfDomain::Above),
        };
    }

    private function edge(int $value): int|OutOfDomain
    {
        return $value < 0 ? OutOfDomain::Below : $this->converter->toParquetType($value);
    }
}
