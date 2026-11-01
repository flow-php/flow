<?php

declare(strict_types=1);

namespace Flow\Parquet\Filter\Evaluation;

use Flow\Parquet\Data\PhysicalOrder;
use Flow\Parquet\ParquetFile\ColumnOrder;
use Flow\Parquet\ParquetFile\Schema\FlatColumn;
use Flow\Parquet\ParquetFile\Schema\PhysicalType;

/**
 * $allNaN: every non-null value is NaN; $min and $max are null when the column order trusts none.
 */
final readonly class ZoneMap
{
    public function __construct(
        public bool|float|int|string|null $min,
        public bool|float|int|string|null $max,
        public bool $minExact,
        public bool $maxExact,
        public ?int $nullCount,
        public ?int $nanCount,
        public int $rowCount,
        public bool $allNaN,
    ) {}

    /**
     * Under IEEE_754_TOTAL_ORDER a writer stores a NaN bound only when every value that is not null is NaN.
     */
    public static function fromStatistics(
        FlatColumn $column,
        ColumnOrder $columnOrder,
        bool|float|int|string|null $min,
        bool|float|int|string|null $max,
        ?bool $minExact,
        ?bool $maxExact,
        ?int $nullCount,
        ?int $nanCount,
        int $rowCount,
    ): self {
        $order = PhysicalOrder::forColumn($column);
        $min = $columnOrder->trustedMin($column, $min);
        $max = $columnOrder->trustedMax($column, $max);
        $truncatable =
            $column->type() === PhysicalType::BYTE_ARRAY || $column->type() === PhysicalType::FIXED_LEN_BYTE_ARRAY;

        return new self(
            min: $min,
            max: $max,
            minExact: $minExact ?? !$truncatable,
            maxExact: $maxExact ?? !$truncatable,
            nullCount: $nullCount,
            nanCount: $nanCount,
            rowCount: $rowCount,
            allNaN: $columnOrder === ColumnOrder::Ieee754TotalOrder
            && ($min !== null && $order->isNan($min) || $max !== null && $order->isNan($max)),
        );
    }
}
