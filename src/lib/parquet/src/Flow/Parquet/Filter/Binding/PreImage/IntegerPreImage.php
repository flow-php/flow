<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Binding\PreImage;

use DateTimeImmutable;
use Flow\Parquet\Filter\Binding\EqualRange;
use Flow\Parquet\Filter\Binding\PreImage;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\LogicalType\Integer;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

use function abs;
use function ceil;
use function floor;
use function is_float;
use function is_int;

use const PHP_INT_MAX;
use const PHP_INT_MIN;

/**
 * A float literal compares exactly up to 2^53: a fraction lies between two integers and equals neither.
 */
final readonly class IntegerPreImage implements PreImage
{
    private const int EXACT_FLOAT_INTEGER = 2 ** 53;

    public function __construct(
        private int $min,
        private int $max,
    ) {}

    public static function forColumn(FlatColumn $column): self
    {
        $integer = Integer::forColumn($column);
        $bits = $integer?->bitWidth() ?? ($column->type() === PhysicalType::INT32 ? 32 : 64);

        return match (true) {
            $bits === 64 => new self(PHP_INT_MIN, PHP_INT_MAX),
            $integer?->isSigned() === false => new self(0, 2 ** $bits - 1),
            default => new self(-2 ** ($bits - 1), 2 ** ($bits - 1) - 1),
        };
    }

    public function equal(bool|DateTimeImmutable|float|int|string $literal): ?EqualRange
    {
        if (is_int($literal)) {
            return EqualRange::integers((string) $literal, (string) $literal, $this->min, $this->max);
        }

        if (!is_float($literal) || abs($literal) > self::EXACT_FLOAT_INTEGER) {
            return null;
        }

        return EqualRange::integers(
            (string) (int) ceil($literal),
            (string) (int) floor($literal),
            $this->min,
            $this->max,
        );
    }
}
