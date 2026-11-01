<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\OutOfDomain;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function bcadd;
use function bcpow;
use function bcsub;
use function Flow\Parquet\Binary\decimal_from_unscaled;
use function Flow\Parquet\Binary\decimal_unscaled;
use function is_float;
use function is_infinite;
use function is_int;
use function pack;

use const PHP_INT_MAX;

/**
 * Float decode is exactly invertible only up to precision 15; byte edges are 8 sign-extended big-endian bytes.
 */
final readonly class DecimalPreImage implements PreImage
{
    private const int MAX_EXACT_PRECISION = 15;

    private function __construct(
        private int $max,
        private int $scale,
        private bool $bytes,
    ) {}

    public static function forColumn(FlatColumn $column): ?self
    {
        $decimal = $column->logicalType()?->decimalData();

        if ($decimal === null || $decimal->precision() > self::MAX_EXACT_PRECISION) {
            return null;
        }

        return new self(
            (int) bcsub(bcpow('10', (string) $decimal->precision()), '1'),
            $decimal->scale(),
            $column->type() !== PhysicalType::INT32 && $column->type() !== PhysicalType::INT64,
        );
    }

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        $value = match (true) {
            is_int($literal) => (float) $literal,
            is_float($literal) && !is_infinite($literal) => $literal,
            default => null,
        };

        if ($value === null) {
            return null;
        }

        $unscaled = decimal_unscaled($value, PHP_INT_MAX, $this->scale);
        $decoded = decimal_from_unscaled($unscaled, $this->scale);
        $floor = $decoded <= $value ? $unscaled : bcsub($unscaled, '1');
        $range = EqualRange::integers(
            $decoded === $value ? $floor : bcadd($floor, '1'),
            $floor,
            -$this->max,
            $this->max,
        );

        return $this->bytes ? new EqualRange(self::bytes($range->low), self::bytes($range->high)) : $range;
    }

    private static function bytes(bool|float|int|string|OutOfDomain $edge): bool|float|int|string|OutOfDomain
    {
        return is_int($edge) ? pack('J', $edge) : $edge;
    }
}
